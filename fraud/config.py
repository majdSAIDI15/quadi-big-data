"""Configuration centralisée, surchargeable par variables d'environnement.

Toutes les valeurs « métier » (fenêtres, seuils, topics) sont ici pour que
producteur, job Spark, dashboard et scripts d'évaluation restent cohérents.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    # --- Kafka -------------------------------------------------------------
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9094"))
    topic_transactions: str = field(default_factory=lambda: _env("TOPIC_TRANSACTIONS", "transactions"))
    topic_alerts: str = field(default_factory=lambda: _env("TOPIC_ALERTS", "fraud-alerts"))
    topic_ground_truth: str = field(default_factory=lambda: _env("TOPIC_GROUND_TRUTH", "ground-truth"))

    # --- Lakehouse ---------------------------------------------------------
    lakehouse_dir: str = field(default_factory=lambda: _env("LAKEHOUSE_DIR", "./data/lakehouse"))
    checkpoint_dir: str = field(default_factory=lambda: _env("CHECKPOINT_DIR", "./data/checkpoints"))
    metrics_dir: str = field(default_factory=lambda: _env("METRICS_DIR", "./data/metrics"))

    # --- Spark -------------------------------------------------------------
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[*]"))
    spark_driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "2g"))
    shuffle_partitions: int = field(default_factory=lambda: _env_int("SPARK_SHUFFLE_PARTITIONS", 4))

    # --- Fenêtrage (exigences du sujet) ------------------------------------
    window_minutes: int = field(default_factory=lambda: _env_int("WINDOW_MINUTES", 10))
    watermark_minutes: int = field(default_factory=lambda: _env_int("WATERMARK_MINUTES", 2))
    slide_minutes: int = field(default_factory=lambda: _env_int("SLIDE_MINUTES", 1))
    slow_path_seconds: int = field(default_factory=lambda: _env_int("SLOW_PATH_SECONDS", 90))

    # --- Détection ---------------------------------------------------------
    high_value_mad: float = field(default_factory=lambda: _env_float("HIGH_VALUE_MAD", 20_000.0))
    mule_min_senders: int = field(default_factory=lambda: _env_int("MULE_MIN_SENDERS", 6))
    mule_min_inflow_mad: float = field(default_factory=lambda: _env_float("MULE_MIN_INFLOW_MAD", 30_000.0))
    mule_flow_through_min: float = field(default_factory=lambda: _env_float("MULE_FLOW_THROUGH_MIN", 0.75))
    scatter_min_intermediaries: int = field(default_factory=lambda: _env_int("SCATTER_MIN_INTERMEDIARIES", 3))
    pivot_top_fraction: float = field(default_factory=lambda: _env_float("PIVOT_TOP_FRACTION", 0.01))
    pivot_min_cycles: int = field(default_factory=lambda: _env_int("PIVOT_MIN_CYCLES", 2))
    # Alertes publiées dans Kafka à partir de ce niveau (toutes vont dans Delta).
    kafka_min_level: str = field(default_factory=lambda: _env("KAFKA_MIN_LEVEL", "MEDIUM"))

    @property
    def window_seconds(self) -> int:
        return self.window_minutes * 60

    def table(self, name: str) -> str:
        return f"{self.lakehouse_dir.rstrip('/')}/{TABLES[name]}"


# Chemins relatifs au lakehouse (architecture médaillon).
TABLES = {
    "transactions": "bronze/transactions",
    "quarantine": "bronze/transactions_quarantine",
    "account_metrics": "silver/account_metrics",
    "traffic_windows": "silver/traffic_windows",
    "fraud_alerts": "gold/fraud_alerts",
    "subgraph_vertices": "gold/anomaly_subgraph_vertices",
    "subgraph_edges": "gold/anomaly_subgraph_edges",
}

RISK_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]

settings = Settings()
