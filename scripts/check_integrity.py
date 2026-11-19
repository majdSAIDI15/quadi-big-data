"""Vérifie l'intégrité du lakehouse (preuve de l'exactly-once après une panne).

    docker compose exec dashboard python scripts/check_integrity.py

Contrôle : doublons de transaction_id dans le bronze, doublons d'alert_id dans le gold,
cohérence alertes <-> sous-graphes, contraintes métier et version des tables.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fraud import dashboard_data as data  # noqa: E402
from fraud.config import TABLES  # noqa: E402


def main() -> int:
    ok = True
    print(f"{'table':<38} {'version':>8} {'lignes':>10}")
    for key, path in TABLES.items():
        t = data.delta(key)
        n = len(data.read(key, days=3, columns=["dt"])) if t is not None else 0
        print(f"{path:<38} {t.version() if t is not None else '-':>8} {n:>10}")

    tx = data.read("transactions", columns=["transaction_id", "montant", "source_account", "target_account"], days=3)
    dup_tx = int(tx["transaction_id"].duplicated().sum()) if not tx.empty else 0
    bad = int(((tx["montant"] <= 0) | (tx["source_account"] == tx["target_account"])).sum()) if not tx.empty else 0
    al = data.read("fraud_alerts", columns=["alert_id"], days=3)
    dup_al = int(al["alert_id"].duplicated().sum()) if not al.empty else 0
    v = data.read("subgraph_vertices", columns=["alert_id"], days=3)
    orphans = len(set(v["alert_id"]) - set(al["alert_id"])) if not v.empty else 0

    checks = [
        ("Transactions en double dans le bronze", dup_tx),
        ("Transactions violant les contraintes métier", bad),
        ("Alertes en double dans le gold", dup_al),
        ("Sous-graphes sans alerte (orphelins)", orphans),
    ]
    print()
    for label, value in checks:
        status = "OK " if value == 0 else "KO "
        ok &= value == 0
        print(f"[{status}] {label} : {value}")
    print(f"\n{len(tx)} transactions, {len(al)} alertes — intégrité {'vérifiée' if ok else 'NON vérifiée'}.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
