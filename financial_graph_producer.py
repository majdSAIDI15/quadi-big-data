"""Livrable 1 — Générateur de transactions financières et injection de cycles de blanchiment.

Publie dans Kafka (topic `transactions`) un flux JSON conforme au sujet :
    {"transaction_id", "source_account", "target_account", "montant", "devise",
     "type_operation", "timestamp"}

et, pour chaque fraude injectée, la vérité terrain dans le topic `ground-truth`
(ce qui permet de mesurer précision, rappel et latence de détection).

Exemples :
    python financial_graph_producer.py                         # flux continu 100 tx/s
    python financial_graph_producer.py --rate 500 --fraud-every 20
    python financial_graph_producer.py --inject CARROUSEL --tempo 0.05   # une fraude, rapide
    python financial_graph_producer.py --dry-run --duration 2            # affiche sans Kafka
"""
from __future__ import annotations

import argparse
import heapq
import itertools
import json
import random
import signal
import sys
import time

from fraud.config import settings
from fraud.scenarios import (
    SCENARIO_TYPES,
    AccountPopulation,
    Scenario,
    build_scenario,
    malformed_transaction,
)

DEFAULT_WEIGHTS = {"CARROUSEL": 0.35, "CYCLE_LONG": 0.15, "FAN_IN_MULE": 0.20,
                   "SCATTER_GATHER": 0.15, "PIVOT_HUB": 0.15}


class FinancialGraphProducer:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.pop = AccountPopulation(args.accounts, seed=args.seed)
        self.rng = random.Random(None if args.seed is None else args.seed + 1)
        self.pending: list[tuple[float, int, dict]] = []  # tas : (échéance, n°, transaction)
        self.seq = itertools.count()
        self.stats = {"normal": 0, "fraud": 0, "late": 0, "malformed": 0, "scenarios": 0}
        self.running = True
        if args.dry_run:
            self.producer = None
        else:
            from fraud.kafka_io import make_producer
            self.producer = make_producer(args.bootstrap)

    # -- émission ---------------------------------------------------------------
    def emit(self, txn: dict, delay_seconds: float = 0.0) -> None:
        if self.producer is None:
            from fraud.scenarios import iso, utc_now
            print(json.dumps({**txn, "timestamp": iso(utc_now())}, ensure_ascii=False))
            return
        from fraud.kafka_io import send_transaction
        send_transaction(self.producer, txn, delay_seconds, self.args.topic)

    def emit_malformed(self) -> None:
        txn = malformed_transaction(self.pop)
        if self.producer is None:
            print("MALFORMED", txn)
        else:
            from fraud.kafka_io import send_transaction
            send_transaction(self.producer, txn, 0.0, self.args.topic)

    def inject(self, scenario: Scenario) -> None:
        now = time.monotonic()
        for offset, txn in scenario.steps:
            heapq.heappush(self.pending, (now + offset, next(self.seq), txn))
        self.stats["scenarios"] += 1
        if self.producer is not None:
            from fraud.kafka_io import publish_ground_truth
            publish_ground_truth(self.producer, scenario)
        print(f"[inject] {scenario.scenario_type:<15} {scenario.scenario_id}  "
              f"{len(scenario.steps)} tx, {scenario.total_amount:,.0f} MAD, "
              f"sur {scenario.duration_seconds:.0f} s — {scenario.description}", flush=True)

    def flush_due(self) -> None:
        now = time.monotonic()
        while self.pending and self.pending[0][0] <= now:
            _, _, txn = heapq.heappop(self.pending)
            self.emit(txn)
            self.stats["fraud"] += 1

    def random_scenario(self) -> Scenario:
        kinds = [k for k in self.args.scenarios]
        weights = [DEFAULT_WEIGHTS[k] for k in kinds]
        return build_scenario(self.rng.choices(kinds, weights=weights)[0], self.pop, self.args.tempo)

    # -- boucle principale --------------------------------------------------------
    def run(self) -> None:
        a = self.args
        start = last_report = time.monotonic()
        auto_fraud = a.fraud_every > 0 and a.inject is None
        next_fraud = start + a.first_fraud_after if auto_fraud else float("inf")
        sent_normal = 0
        tick = 0.02
        while self.running:
            now = time.monotonic()
            if a.duration and now - start >= a.duration and not self.pending:
                break
            if a.inject is None and a.rate > 0 and (not a.duration or now - start < a.duration):
                target = int((now - start) * a.rate)
                while sent_normal < target:
                    r = self.rng.random()
                    if r < a.malformed_ratio:
                        self.emit_malformed()
                        self.stats["malformed"] += 1
                    elif r < a.malformed_ratio + a.late_ratio:
                        # Moitié « en retard mais acceptée », moitié au-delà du watermark (rejetée).
                        delay = self.rng.uniform(30, 100) if self.rng.random() < 0.5 else self.rng.uniform(150, 300)
                        self.emit(self.pop.normal_transaction(), delay)
                        self.stats["late"] += 1
                    else:
                        self.emit(self.pop.normal_transaction())
                        self.stats["normal"] += 1
                    sent_normal += 1
            if now >= next_fraud:
                self.inject(self.random_scenario())
                next_fraud = now + a.fraud_every
            self.flush_due()
            if now - last_report >= 10:
                rate = sent_normal / max(now - start, 1e-9)
                print(f"[stats] {rate:7.1f} tx/s | " + " ".join(f"{k}={v}" for k, v in self.stats.items()),
                      flush=True)
                last_report = now
            if a.inject is not None and not self.pending:
                break
            time.sleep(tick)
        if self.producer is not None:
            self.producer.flush(10)
        print("[done] " + " ".join(f"{k}={v}" for k, v in self.stats.items()), flush=True)

    def stop(self, *_):
        self.running = False


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=settings.kafka_bootstrap)
    ap.add_argument("--topic", default=settings.topic_transactions)
    ap.add_argument("--rate", type=float, default=100.0, help="transactions normales par seconde")
    ap.add_argument("--duration", type=float, default=0, help="durée en secondes (0 = infini)")
    ap.add_argument("--accounts", type=int, default=10_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--fraud-every", type=float, default=45.0, help="une fraude toutes les N s (0 = aucune)")
    ap.add_argument("--first-fraud-after", type=float, default=20.0)
    ap.add_argument("--scenarios", nargs="+", default=SCENARIO_TYPES, choices=SCENARIO_TYPES)
    ap.add_argument("--tempo", type=float, default=1.0,
                    help="facteur de temps entre étapes d'une fraude (1 = réaliste, 0.05 = démo)")
    ap.add_argument("--late-ratio", type=float, default=0.002, help="part d'événements en retard")
    ap.add_argument("--malformed-ratio", type=float, default=0.0005, help="part de messages invalides")
    ap.add_argument("--inject", choices=SCENARIO_TYPES, help="injecte une seule fraude puis s'arrête")
    ap.add_argument("--count", type=int, default=1, help="nombre de fraudes avec --inject")
    ap.add_argument("--dry-run", action="store_true", help="affiche au lieu d'envoyer à Kafka")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = parse_args(argv)
    app = FinancialGraphProducer(args)
    signal.signal(signal.SIGINT, app.stop)
    signal.signal(signal.SIGTERM, app.stop)
    if args.inject:
        for _ in range(args.count):
            app.inject(build_scenario(args.inject, app.pop, args.tempo))
    print(f"[start] bootstrap={args.bootstrap} rate={args.rate}/s accounts={args.accounts} "
          f"fraud_every={args.fraud_every}s tempo={args.tempo}", flush=True)
    app.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
