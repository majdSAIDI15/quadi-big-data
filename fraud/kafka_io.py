"""Aides Kafka partagées par le producteur et le dashboard (bouton d'injection)."""
from __future__ import annotations

import json
import threading
import time
from datetime import timedelta

from confluent_kafka import Producer

from .config import settings
from .scenarios import Scenario, iso, utc_now


def make_producer(bootstrap: str | None = None, client_id: str = "financial-graph-producer") -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap or settings.kafka_bootstrap,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "compression.type": "lz4",
    })


def send_transaction(producer: Producer, txn: dict, delay_seconds: float = 0.0,
                     topic: str | None = None) -> dict:
    """Publie une transaction ; `timestamp` = maintenant (moins un retard simulé)."""
    txn = dict(txn)
    txn["timestamp"] = iso(utc_now() - timedelta(seconds=delay_seconds))
    producer.produce(topic or settings.topic_transactions,
                     key=str(txn.get("source_account", "")), value=json.dumps(txn))
    producer.poll(0)
    return txn


def publish_ground_truth(producer: Producer, scenario: Scenario, topic: str | None = None) -> None:
    producer.produce(topic or settings.topic_ground_truth, key=scenario.scenario_id,
                     value=json.dumps(scenario.ground_truth(), ensure_ascii=False))
    producer.poll(0)


def inject_in_background(producer: Producer, scenario: Scenario) -> threading.Thread:
    """Rejoue les étapes d'un scénario en respectant leurs décalages (thread démon)."""
    def run() -> None:
        publish_ground_truth(producer, scenario)
        start = time.monotonic()
        for offset, txn in sorted(scenario.steps, key=lambda s: s[0]):
            wait = start + offset - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            send_transaction(producer, txn)
        producer.flush(5)

    th = threading.Thread(target=run, daemon=True, name=f"inject-{scenario.scenario_id}")
    th.start()
    return th
