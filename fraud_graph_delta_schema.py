"""Livrable 3 — Schéma DDL Delta Lake pour l'audit et l'historisation des sous-graphes.

Architecture médaillon :
  bronze  transactions brutes validées (+ quarantaine des messages invalides)
  silver  métriques de graphe par compte (PageRank, composantes) et trafic fenêtré
  gold    alertes de fraude et sous-graphes d'anomalies (piste d'audit immuable)

Les tables gold sont partitionnées par `risk_level` puis `dt=YYYY-MM-DD`, comme
demandé par le sujet, et déclarées `appendOnly` : une alerte ne peut être ni
modifiée ni supprimée, ce qui en fait une piste d'audit opposable.

Usage :
    python fraud_graph_delta_schema.py               # crée les tables manquantes
    python fraud_graph_delta_schema.py --print-ddl   # affiche le DDL (pour le rapport)
    python fraud_graph_delta_schema.py --describe    # affiche schéma, partitions et historique
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from fraud.config import TABLES, settings


def _sql_str(text: str) -> str:
    """Littéral de chaîne Spark SQL : barres obliques inverses et apostrophes échappées."""
    return text.replace("\\", "\\\\").replace("'", "\\'")


@dataclass(frozen=True)
class Col:
    name: str
    type: str
    nullable: bool = True
    comment: str = ""


@dataclass(frozen=True)
class TableDef:
    key: str
    comment: str
    columns: list[Col]
    partitioned_by: tuple[str, ...]
    properties: dict
    constraints: dict

    def ddl_columns(self) -> str:
        parts = []
        for c in self.columns:
            s = f"  {c.name} {c.type}" + ("" if c.nullable else " NOT NULL")
            if c.comment:
                s += " COMMENT '" + _sql_str(c.comment) + "'"
            parts.append(s)
        return ",\n".join(parts)

    def spark_schema(self) -> str:
        """Schéma DDL compact utilisable par spark.createDataFrame(rows, schema=...)."""
        return ", ".join(f"{c.name} {c.type}" for c in self.columns)

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    def create_sql(self, path: str) -> str:
        props = ",\n".join(f"  '{k}' = '{v}'" for k, v in self.properties.items())
        return (f"CREATE TABLE IF NOT EXISTS delta.`{path}` (\n{self.ddl_columns()}\n)\nUSING DELTA\n"
                f"PARTITIONED BY ({', '.join(self.partitioned_by)})\nCOMMENT '{_sql_str(self.comment)}'\n"
                f"TBLPROPERTIES (\n{props}\n)")


AUDIT_PROPS = {
    "delta.appendOnly": "true",
    "delta.logRetentionDuration": "interval 365 days",
    "delta.deletedFileRetentionDuration": "interval 30 days",
}
OPS_PROPS = {"delta.logRetentionDuration": "interval 30 days"}

DEFS: dict[str, TableDef] = {
    "transactions": TableDef(
        "transactions", "Bronze - transactions Kafka validées et dédoublonnées",
        [
            Col("transaction_id", "STRING", False, "Identifiant unique (clé de dédoublonnage)"),
            Col("source_account", "STRING", False, "Compte émetteur (sommet source)"),
            Col("target_account", "STRING", False, "Compte bénéficiaire (sommet cible)"),
            Col("montant", "DOUBLE", False, "Montant de la transaction"),
            Col("devise", "STRING", True, "Devise ISO 4217"),
            Col("type_operation", "STRING", True, "Nature de l'opération"),
            Col("event_time", "TIMESTAMP", False, "Horodatage métier (temps événement)"),
            Col("kafka_partition", "INT"),
            Col("kafka_offset", "BIGINT"),
            Col("kafka_timestamp", "TIMESTAMP", True, "Horodatage d'ingestion Kafka"),
            Col("ingested_at", "TIMESTAMP", True, "Écriture dans le lakehouse"),
            Col("dt", "STRING", False, "Partition jour YYYY-MM-DD"),
        ],
        ("dt",), OPS_PROPS,
        {"montant_positif": "montant > 0", "pas_auto_virement": "source_account <> target_account"},
    ),
    "quarantine": TableDef(
        "quarantine", "Bronze - messages rejetés par le contrôle qualité",
        [
            Col("raw_value", "STRING", True, "Message Kafka brut"),
            Col("error", "STRING", False, "Motif du rejet"),
            Col("kafka_partition", "INT"),
            Col("kafka_offset", "BIGINT"),
            Col("kafka_timestamp", "TIMESTAMP"),
            Col("ingested_at", "TIMESTAMP"),
            Col("dt", "STRING", False),
        ],
        ("dt",), OPS_PROPS, {},
    ),
    "account_metrics": TableDef(
        "account_metrics", "Silver - métriques de graphe par compte et par instantané",
        [
            Col("snapshot_id", "STRING", False, "Identifiant de l'instantané du chemin lent"),
            Col("snapshot_time", "TIMESTAMP", False),
            Col("account_id", "STRING", False),
            Col("pagerank", "DOUBLE", True, "PageRank sur le graphe des gros flux"),
            Col("pagerank_pct", "DOUBLE", True, "Percentile du PageRank (0-1)"),
            Col("in_degree", "INT"),
            Col("out_degree", "INT"),
            Col("inflow", "DOUBLE", True, "Montant reçu sur la fenêtre"),
            Col("outflow", "DOUBLE", True, "Montant émis sur la fenêtre"),
            Col("flow_through", "DOUBLE", True, "outflow / inflow (compte de passage si proche de 1)"),
            Col("scc_id", "STRING", True, "Composante fortement connexe (graphe des gros flux)"),
            Col("scc_size", "INT"),
            Col("cluster_id", "STRING", True, "Composante connexe du graphe suspect"),
            Col("n_alerts", "INT", True, "Alertes impliquant le compte sur la fenêtre"),
            Col("dt", "STRING", False),
        ],
        ("dt",), OPS_PROPS, {},
    ),
    "traffic_windows": TableDef(
        "traffic_windows", "Silver - trafic agrégé en fenêtre glissante 10 min (pas 1 min)",
        [
            Col("window_start", "TIMESTAMP", False),
            Col("window_end", "TIMESTAMP", False),
            Col("tx_count", "BIGINT"),
            Col("total_amount", "DOUBLE"),
            Col("high_value_count", "BIGINT"),
            Col("active_accounts", "BIGINT", True, "Émetteurs distincts (approx.)"),
            Col("updated_at", "TIMESTAMP"),
            Col("dt", "STRING", False),
        ],
        ("dt",), OPS_PROPS, {},
    ),
    "fraud_alerts": TableDef(
        "fraud_alerts", "Gold - alertes de fraude (piste d'audit immuable)",
        [
            Col("alert_id", "STRING", False, "Empreinte déterministe : idempotence en cas de rejeu"),
            Col("alert_type", "STRING", False, "CYCLE, CYCLE_LONG, MULE, SCATTER_GATHER, PIVOT, CLUSTER"),
            Col("risk_score", "INT", False, "Score 0-100"),
            Col("risk_level", "STRING", False, "LOW, MEDIUM, HIGH, CRITICAL"),
            Col("accounts", "ARRAY<STRING>", False),
            Col("transaction_ids", "ARRAY<STRING>"),
            Col("total_amount", "DOUBLE"),
            Col("devise", "STRING"),
            Col("cycle_length", "INT"),
            Col("window_start", "TIMESTAMP", True, "Première transaction du motif"),
            Col("window_end", "TIMESTAMP", True, "Dernière transaction du motif"),
            Col("detected_at", "TIMESTAMP", False),
            Col("latency_ms", "BIGINT", True, "Délai ingestion Kafka -> alerte"),
            Col("explanation", "STRING", True, "Explication en langage clair"),
            Col("features", "STRING", True, "Indicateurs du score (JSON)"),
            Col("detector", "STRING", True, "fast-path ou slow-path"),
            Col("dt", "STRING", False),
        ],
        ("risk_level", "dt"), AUDIT_PROPS,
        {"score_borne": "risk_score BETWEEN 0 AND 100",
         "niveau_valide": "risk_level IN ('LOW','MEDIUM','HIGH','CRITICAL')"},
    ),
    "subgraph_vertices": TableDef(
        "subgraph_vertices", "Gold - sommets des sous-graphes d'anomalies",
        [
            Col("alert_id", "STRING", False),
            Col("account_id", "STRING", False),
            Col("role", "STRING", True, "ORIGINE, RELAIS, MULE, PIVOT, SOURCE, DESTINATION, MEMBRE"),
            Col("pagerank", "DOUBLE"),
            Col("risk_level", "STRING", False),
            Col("dt", "STRING", False),
        ],
        ("risk_level", "dt"), AUDIT_PROPS, {},
    ),
    "subgraph_edges": TableDef(
        "subgraph_edges", "Gold - arêtes (transactions) des sous-graphes d'anomalies",
        [
            Col("alert_id", "STRING", False),
            Col("transaction_id", "STRING", False),
            Col("src", "STRING", False),
            Col("dst", "STRING", False),
            Col("montant", "DOUBLE"),
            Col("event_time", "TIMESTAMP"),
            Col("type_operation", "STRING"),
            Col("risk_level", "STRING", False),
            Col("dt", "STRING", False),
        ],
        ("risk_level", "dt"), AUDIT_PROPS, {},
    ),
}

assert set(DEFS) == set(TABLES), "DEFS et config.TABLES doivent lister les mêmes tables"


def ensure_schema(spark, lakehouse: str | None = None, verbose: bool = True) -> None:
    """Crée les tables absentes (idempotent) et leurs contraintes CHECK."""
    from delta.tables import DeltaTable

    base = (lakehouse or settings.lakehouse_dir).rstrip("/")
    for key, d in DEFS.items():
        path = f"{base}/{TABLES[key]}"
        existed = DeltaTable.isDeltaTable(spark, path)
        spark.sql(d.create_sql(path))
        if not existed:
            for name, expr in d.constraints.items():
                spark.sql(f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {name} CHECK ({expr})")
        if verbose:
            print(f"[schema] {'existe ' if existed else 'créée  '} {TABLES[key]}")


def describe(spark, lakehouse: str | None = None) -> None:
    base = (lakehouse or settings.lakehouse_dir).rstrip("/")
    for key in DEFS:
        path = f"{base}/{TABLES[key]}"
        print(f"\n=== {TABLES[key]}")
        spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select(
            "partitionColumns", "numFiles", "sizeInBytes", "properties").show(truncate=False)
        spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select(
            "version", "timestamp", "operation").show(5, truncate=False)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lakehouse", default=settings.lakehouse_dir)
    ap.add_argument("--print-ddl", action="store_true", help="affiche le DDL sans Spark")
    ap.add_argument("--describe", action="store_true")
    args = ap.parse_args()

    if args.print_ddl:
        for key, d in DEFS.items():
            print(d.create_sql(f"{args.lakehouse}/{TABLES[key]}") + ";")
            for name, expr in d.constraints.items():
                print(f"ALTER TABLE delta.`{args.lakehouse}/{TABLES[key]}` ADD CONSTRAINT {name} CHECK ({expr});")
            print()
        return

    from fraud.spark_session import build_spark

    spark = build_spark("fraud-graph-delta-schema")
    ensure_schema(spark, args.lakehouse)
    if args.describe:
        describe(spark, args.lakehouse)
    spark.stop()


if __name__ == "__main__":
    main()
