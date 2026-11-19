"""Micro-benchmark des primitives GraphFrames utilisées par le pipeline.

Usage (conteneur) : docker compose run --rm --no-deps spark-streaming python scripts/microbench_graphframes.py
"""
from __future__ import annotations

import os
import random
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("CHECKPOINT_DIR", "/tmp/mb-ckpt")

from fraud import detection as D  # noqa: E402
from fraud.spark_session import build_spark  # noqa: E402

SCHEMA = ("transaction_id STRING, src STRING, dst STRING, montant DOUBLE, event_time TIMESTAMP, "
          "type_operation STRING, kafka_timestamp TIMESTAMP")


def edges(spark, n_edges: int, n_nodes: int, seed: int = 1):
    rng = random.Random(seed)
    t0 = datetime(2026, 1, 1)
    rows = []
    for i in range(n_edges):
        a, b = rng.randrange(n_nodes), rng.randrange(n_nodes)
        if a == b:
            continue
        t = t0 + timedelta(seconds=rng.uniform(0, 600))
        rows.append((f"T{i}", f"A{a}", f"A{b}", rng.uniform(100, 200_000), t, "X", t))
    return spark.createDataFrame(rows, SCHEMA)


def timed(label, fn, repeat=3):
    out = None
    best = float("inf")
    for _ in range(repeat):
        t = time.time()
        out = fn()
        best = min(best, time.time() - t)
    print(f"{label:<45} {best * 1000:8.0f} ms", flush=True)
    return out


def main():
    spark = build_spark("microbench", extra_conf=dict(a.split("=", 1) for a in sys.argv[1:]))
    small = edges(spark, 60, 40).cache()
    small.count()
    timed("find triangles, 60 arêtes", lambda: D.find_cycles(small, 3, 600).collect())
    big = edges(spark, 6_000, 2_000).cache()
    big.count()
    timed("find triangles, 6k arêtes", lambda: D.find_cycles(big, 3, 600).collect())
    hv = big.filter("montant >= 100000").cache()
    print("arêtes gros flux :", hv.count())
    timed("pageRank 10 it, gros flux", lambda: D.pagerank(hv).collect(), 2)
    timed("stronglyConnectedComponents, gros flux", lambda: D.strongly_connected(hv).collect(), 2)
    timed("connectedComponents graphx, 6k arêtes", lambda: D.connected_components(big).collect(), 2)
    sus = big.limit(80).cache()
    sus.count()
    timed("connectedComponents graphx, 80 arêtes", lambda: D.connected_components(sus).collect(), 2)
    timed("scatter-gather, 6k arêtes", lambda: D.find_scatter_gather(big, 3, 600).collect(), 2)
    timed("mules, 6k arêtes", lambda: D.find_mules(big, 6, 30_000, 0.8).collect(), 2)
    spark.stop()


if __name__ == "__main__":
    main()
