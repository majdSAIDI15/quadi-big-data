"""Génération de transactions : trafic légitime + scénarios de blanchiment.

Module pur Python (aucune dépendance Kafka/Spark) : il est réutilisé par le
producteur, par le bouton d'injection du dashboard et par les tests.

La typologie des scénarios s'inspire des motifs de blanchiment du jeu de données
IBM AML (Altman et al., NeurIPS 2023) : cycle, fan-in, scatter-gather, plus un
scénario « pivot » (un compte au centre de plusieurs carrousels).
"""
from __future__ import annotations

import itertools
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

DEVISE = "MAD"

# type_operation -> (poids dans le trafic normal, montant médian en MAD)
OPERATIONS: dict[str, tuple[float, float]] = {
    "VIREMENT_PARTICULIER": (0.35, 1_500.0),
    "PAIEMENT_FOURNISSEUR": (0.20, 8_000.0),
    "VIREMENT_COMMERCIAL": (0.15, 12_000.0),
    "REGLEMENT_FACTURE": (0.20, 3_000.0),
    "TRANSFERT_INTERNE": (0.10, 2_500.0),
}

SCENARIO_TYPES = ["CARROUSEL", "CYCLE_LONG", "FAN_IN_MULE", "SCATTER_GATHER", "PIVOT_HUB"]

# Alerte(s) attendue(s) par type de scénario (sert à l'évaluation).
EXPECTED_ALERTS = {
    "CARROUSEL": ["CYCLE"],
    "CYCLE_LONG": ["CYCLE_LONG"],
    "FAN_IN_MULE": ["MULE"],
    "SCATTER_GATHER": ["SCATTER_GATHER"],
    "PIVOT_HUB": ["PIVOT", "CYCLE"],
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def new_txn_id(ts: datetime | None = None) -> str:
    year = (ts or utc_now()).year
    return f"TXN-{year}-{uuid.uuid4().hex[:12].upper()}"


def make_txn(src: str, dst: str, montant: float, op: str, ts: datetime | None = None,
             txn_id: str | None = None) -> dict:
    ts = ts or utc_now()
    return {
        "transaction_id": txn_id or new_txn_id(ts),
        "source_account": src,
        "target_account": dst,
        "montant": round(float(montant), 2),
        "devise": DEVISE,
        "type_operation": op,
        "timestamp": iso(ts),
    }


class AccountPopulation:
    """Population de comptes avec une activité réaliste.

    - activité d'émission en loi de puissance (quelques comptes très actifs) ;
    - 2 % de « commerçants » qui reçoivent beaucoup (hubs légitimes : ils ont un
      PageRank élevé sans être frauduleux, ce qui rend la détection honnête) ;
    - chaque compte a des bénéficiaires habituels (fournisseurs, proches).
    """

    def __init__(self, n_accounts: int = 10_000, seed: int | None = 42):
        self.rng = random.Random(seed)
        width = max(4, len(str(n_accounts - 1)))
        self.accounts = [f"ACC-{i:0{width}d}" for i in range(n_accounts)]
        rng = self.rng
        n_merchants = max(5, n_accounts // 50)
        self.merchants = rng.sample(self.accounts, n_merchants)
        merchant_set = set(self.merchants)
        self.individuals = [a for a in self.accounts if a not in merchant_set]

        weights = [rng.paretovariate(1.6) for _ in self.accounts]
        self._cum_send = list(itertools.accumulate(weights))

        self.prefs: dict[str, list[str]] = {}
        for acc in self.accounts:
            k = rng.randint(3, 12)
            if acc in merchant_set:
                # Les commerçants paient surtout des particuliers / fournisseurs.
                self.prefs[acc] = rng.sample(self.individuals, k)
            else:
                n_m = int(k * 0.4)
                self.prefs[acc] = rng.sample(self.merchants, n_m) + rng.sample(self.individuals, k - n_m)
            self.prefs[acc] = [p for p in self.prefs[acc] if p != acc] or [self.individuals[0]]

        # Comptes « dormants » : le tiers le moins actif, en émission ET en réception
        # (un compte rarement payé par ses habitués). Ils servent de sociétés écrans et de
        # mules : une vraie mule est un compte calme, pas un compte très sollicité.
        incoming: dict[str, float] = dict.fromkeys(self.accounts, 0.0)
        for i, acc in enumerate(self.accounts):
            share = weights[i] / len(self.prefs[acc])
            for p in self.prefs[acc]:
                incoming[p] += share
        activity = {a: weights[i] + incoming[a] for i, a in enumerate(self.accounts)}
        self.dormant = sorted(self.individuals, key=lambda a: activity[a])[: n_accounts // 3]

        self._ops = list(OPERATIONS)
        self._op_weights = [OPERATIONS[o][0] for o in self._ops]
        self._reserved: dict[str, datetime] = {}

    # -- trafic normal ----------------------------------------------------------
    def normal_amount(self, op: str) -> float:
        median = OPERATIONS[op][1]
        amount = self.rng.lognormvariate(0.0, 1.0) * median
        return min(max(amount, 50.0), 500_000.0)

    def normal_transaction(self, ts: datetime | None = None) -> dict:
        rng = self.rng
        src = rng.choices(self.accounts, cum_weights=self._cum_send)[0]
        dst = rng.choice(self.prefs[src]) if rng.random() < 0.85 else rng.choice(self.accounts)
        if dst == src:
            dst = rng.choice(self.individuals)
            if dst == src:
                dst = self.accounts[(self.accounts.index(src) + 1) % len(self.accounts)]
        op = rng.choices(self._ops, weights=self._op_weights)[0]
        return make_txn(src, dst, self.normal_amount(op), op, ts)

    # -- sélection de comptes pour la fraude -------------------------------------
    def pick_shell_accounts(self, n: int, hold_minutes: int = 20) -> list[str]:
        """Choisit n comptes dormants distincts, non utilisés récemment par un
        autre scénario (évite que deux fraudes se chevauchent par hasard)."""
        now = utc_now()
        self._reserved = {a: t for a, t in self._reserved.items() if t > now}
        pool = [a for a in self.dormant if a not in self._reserved]
        chosen = self.rng.sample(pool, n)
        for a in chosen:
            self._reserved[a] = now + timedelta(minutes=hold_minutes)
        return chosen


@dataclass
class Scenario:
    scenario_id: str
    scenario_type: str
    accounts: list[str]
    key_accounts: list[str]
    # (décalage en secondes depuis le début du scénario, transaction sans timestamp définitif)
    steps: list[tuple[float, dict]] = field(default_factory=list)
    description: str = ""

    @property
    def transaction_ids(self) -> list[str]:
        return [t["transaction_id"] for _, t in self.steps]

    @property
    def total_amount(self) -> float:
        return round(sum(t["montant"] for _, t in self.steps), 2)

    @property
    def duration_seconds(self) -> float:
        return max((o for o, _ in self.steps), default=0.0)

    def ground_truth(self, injected_at: datetime | None = None) -> dict:
        return {
            "scenario_id": self.scenario_id,
            "scenario_type": self.scenario_type,
            "accounts": self.accounts,
            "key_accounts": self.key_accounts,
            "transaction_ids": self.transaction_ids,
            "total_amount": self.total_amount,
            "expected_alert_types": EXPECTED_ALERTS[self.scenario_type],
            "description": self.description,
            "injected_at": iso(injected_at or utc_now()),
        }


def _scenario_id(kind: str) -> str:
    return f"SCN-{kind[:4]}-{uuid.uuid4().hex[:8].upper()}"


def _gap(rng: random.Random, tempo: float, lo: float = 20.0, hi: float = 90.0) -> float:
    return rng.uniform(lo, hi) * tempo


def build_cycle(pop: AccountPopulation, length: int = 3, tempo: float = 1.0,
                accounts: list[str] | None = None, base_amount: float | None = None,
                kind: str | None = None) -> Scenario:
    """Carrousel : l'argent fait le tour de `length` sociétés écrans et revient à
    l'origine. Chaque étape prélève 1 à 3 % de « commission » (signature typique)."""
    rng = pop.rng
    accs = accounts or pop.pick_shell_accounts(length)
    amount = base_amount or round(rng.uniform(40_000, 250_000), -2)
    kind = kind or ("CARROUSEL" if length == 3 else "CYCLE_LONG")
    steps, offset = [], 0.0
    for i in range(length):
        src, dst = accs[i], accs[(i + 1) % length]
        steps.append((offset, make_txn(src, dst, amount, "VIREMENT_COMMERCIAL")))
        amount *= 1 - rng.uniform(0.01, 0.03)
        offset += _gap(rng, tempo)
    path = " → ".join(accs + [accs[0]])
    return Scenario(_scenario_id(kind), kind, accs, [accs[0]], steps,
                    f"Carrousel {length} comptes : {path}")


def build_fan_in_mule(pop: AccountPopulation, tempo: float = 1.0) -> Scenario:
    """Smurfing : de nombreux petits dépôts (< 10 000 MAD) convergent vers un
    compte mule, qui vide ensuite le compte vers un bénéficiaire."""
    rng = pop.rng
    n = rng.randint(8, 14)
    accs = pop.pick_shell_accounts(n + 2)
    mule, beneficiary, senders = accs[0], accs[1], accs[2:]
    steps, offset, total = [], 0.0, 0.0
    for s in senders:
        amt = round(rng.uniform(2_000, 9_800), 2)
        total += amt
        steps.append((offset, make_txn(s, mule, amt, "VIREMENT_PARTICULIER")))
        offset += _gap(rng, tempo, 5, 25)
    offset += _gap(rng, tempo, 10, 40)
    steps.append((offset, make_txn(mule, beneficiary, total * rng.uniform(0.90, 0.97), "TRANSFERT_INTERNE")))
    return Scenario(_scenario_id("FAN_IN_MULE"), "FAN_IN_MULE", accs, [mule], steps,
                    f"Mule {mule} : {n} dépôts fractionnés puis retrait vers {beneficiary}")


def build_scatter_gather(pop: AccountPopulation, tempo: float = 1.0) -> Scenario:
    """Scatter-gather : une source éclate un montant vers k intermédiaires, qui
    reversent presque tout à un même destinataire."""
    rng = pop.rng
    k = rng.randint(4, 6)
    accs = pop.pick_shell_accounts(k + 2)
    source, sink, mids = accs[0], accs[1], accs[2:]
    total = round(rng.uniform(60_000, 200_000), -2)
    steps = []
    for i, m in enumerate(mids):
        part = total / k * rng.uniform(0.97, 1.03)
        t0 = i * _gap(rng, tempo, 3, 10)
        steps.append((t0, make_txn(source, m, part, "PAIEMENT_FOURNISSEUR")))
        steps.append((t0 + _gap(rng, tempo, 15, 60),
                      make_txn(m, sink, part * rng.uniform(0.97, 0.99), "VIREMENT_COMMERCIAL")))
    steps.sort(key=lambda s: s[0])
    return Scenario(_scenario_id("SCATTER"), "SCATTER_GATHER", accs, [source, sink], steps,
                    f"Scatter-gather {source} → {k} intermédiaires → {sink}")


def build_pivot_hub(pop: AccountPopulation, tempo: float = 1.0) -> Scenario:
    """Un compte pivot au centre de 3 carrousels imbriqués (anneau de blanchiment)."""
    rng = pop.rng
    accs = pop.pick_shell_accounts(7)
    pivot, others = accs[0], accs[1:]
    steps: list[tuple[float, dict]] = []
    start = 0.0
    for i in range(3):
        sub = build_cycle(pop, 3, tempo, accounts=[pivot, others[2 * i], others[2 * i + 1]],
                          base_amount=round(rng.uniform(60_000, 150_000), -2))
        steps += [(start + o, t) for o, t in sub.steps]
        start += _gap(rng, tempo, 10, 40)
    steps.sort(key=lambda s: s[0])
    return Scenario(_scenario_id("PIVOT_HUB"), "PIVOT_HUB", accs, [pivot], steps,
                    f"Pivot {pivot} au centre de 3 carrousels")


BUILDERS: dict[str, Callable[..., Scenario]] = {
    "CARROUSEL": lambda pop, tempo: build_cycle(pop, 3, tempo),
    "CYCLE_LONG": lambda pop, tempo: build_cycle(pop, pop.rng.choice([4, 5]), tempo),
    "FAN_IN_MULE": build_fan_in_mule,
    "SCATTER_GATHER": build_scatter_gather,
    "PIVOT_HUB": build_pivot_hub,
}


def build_scenario(kind: str, pop: AccountPopulation, tempo: float = 1.0) -> Scenario:
    if kind not in BUILDERS:
        raise ValueError(f"Scénario inconnu : {kind}. Choix : {', '.join(BUILDERS)}")
    return BUILDERS[kind](pop, tempo)


def malformed_transaction(pop: AccountPopulation) -> dict:
    """Message invalide (montant négatif, auto-virement ou champ manquant) pour
    démontrer la mise en quarantaine côté lakehouse."""
    t = pop.normal_transaction()
    flaw = pop.rng.choice(["negative", "self", "missing"])
    if flaw == "negative":
        t["montant"] = -abs(t["montant"])
    elif flaw == "self":
        t["target_account"] = t["source_account"]
    else:
        t.pop("target_account")
    return t
