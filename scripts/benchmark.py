"""Benchmark de montée en charge du chemin rapide.

Pour chaque débit (tx/s), injecte du trafic + des carrousels pendant `--duration`
secondes, puis mesure : débit réellement traité, durée des micro-batches et
latence de détection des carrousels (ingestion Kafka -> alerte).

Le producteur continu doit être arrêté pendant la mesure :
    docker compose stop producer
    docker compose run --rm --no-deps producer python scripts/benchmark.py --rates 25 50 100 200
Sorties : docs/benchmark.md, docs/benchmark.json, docs/img/benchmark_*.png
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from financial_graph_producer import FinancialGraphProducer, parse_args  # noqa: E402
from fraud import dashboard_data as data  # noqa: E402

OUT_DIR = os.environ.get("DOCS_DIR", "docs")


def phase(rate: float, duration: int, seed: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    args = parse_args(["--rate", str(rate), "--duration", str(duration), "--fraud-every", "20",
                       "--first-fraud-after", "5", "--scenarios", "CARROUSEL", "--tempo", "0.1",
                       "--seed", str(seed), "--late-ratio", "0", "--malformed-ratio", "0"])
    start = data.utcnow()
    FinancialGraphProducer(args).run()
    return start, data.utcnow()


def measure(start: pd.Timestamp, end: pd.Timestamp, rate: float) -> dict:
    fast = data.jsonl_tail("fast_path.jsonl", 20000)
    f = fast[(fast["ts"] >= start + pd.Timedelta(seconds=20)) & (fast["ts"] <= end + pd.Timedelta(seconds=40))]
    alerts = data.alerts(start)
    cyc = alerts[(alerts["alert_type"] == "CYCLE") & (alerts["risk_level"] != "LOW")] if not alerts.empty else alerts
    cyc = cyc[cyc["detected_at"] <= end + pd.Timedelta(seconds=60)] if not cyc.empty else cyc
    lat = cyc["latency_ms"].dropna() / 1000 if not cyc.empty else pd.Series(dtype=float)
    span = (f["ts"].max() - f["ts"].min()).total_seconds() if len(f) > 1 else 0
    return {
        "rate_target": rate,
        "processed_tx_s": round(float(f["rows"].sum()) / span, 1) if span else None,
        "batches": int(len(f)),
        "batch_rows_median": int(f["rows"].median()) if len(f) else None,
        "batch_ms_p50": int(f["total_ms"].median()) if len(f) else None,
        "batch_ms_p95": int(f["total_ms"].quantile(0.95)) if len(f) else None,
        "carousels_detected": int(len(cyc)),
        "latency_s_p50": round(float(lat.median()), 2) if len(lat) else None,
        "latency_s_p95": round(float(lat.quantile(0.95)), 2) if len(lat) else None,
    }


def charts(res: list[dict]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    df = pd.DataFrame(res)
    os.makedirs(os.path.join(OUT_DIR, "img"), exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "axes.spines.top": False, "axes.spines.right": False})
    fig, ax = plt.subplots(figsize=(6.5, 3.2), dpi=150)
    ax.plot(df["rate_target"], df["latency_s_p50"], marker="o", color="#2a78d6", linewidth=2, label="p50")
    ax.plot(df["rate_target"], df["latency_s_p95"], marker="o", color="#eb6834", linewidth=2, label="p95")
    ax.set_xlabel("Débit injecté (transactions/s)")
    ax.set_ylabel("Latence d'alerte (s)")
    ax.set_title("Latence de détection des carrousels selon la charge", loc="left", fontsize=11)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "img", "benchmark_latence.png"))
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(6.5, 3.2), dpi=150)
    ax.plot(df["rate_target"], df["rate_target"], color="#c3c2b7", linestyle="--", linewidth=1, label="idéal")
    ax.plot(df["rate_target"], df["processed_tx_s"], marker="o", color="#2a78d6", linewidth=2, label="traité")
    ax.set_xlabel("Débit injecté (transactions/s)")
    ax.set_ylabel("Débit traité (tx/s)")
    ax.set_title("Débit soutenu par le chemin rapide", loc="left", fontsize=11)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "img", "benchmark_debit.png"))
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rates", type=float, nargs="+", default=[25, 50, 100, 200])
    ap.add_argument("--duration", type=int, default=180)
    ap.add_argument("--cooldown", type=int, default=60)
    args = ap.parse_args()
    res = []
    for i, rate in enumerate(args.rates):
        print(f"[bench] phase {rate} tx/s pendant {args.duration} s", flush=True)
        start, end = phase(rate, args.duration, 1000 + i)
        time.sleep(args.cooldown)
        r = measure(start, end, rate)
        print(f"[bench] {r}", flush=True)
        res.append(r)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "benchmark.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "results": res}, f, indent=2)
    charts(res)
    lines = ["# Benchmark de montée en charge\n",
             f"_Machine : portable, Docker Desktop (8 vCPU partagés, ~6 Go), Spark local[*]. "
             f"{args.duration} s par palier._\n",
             "| Débit injecté | Débit traité | Batches | Lignes/batch (méd.) | Durée batch p50 | p95 | "
             "Carrousels détectés | Latence p50 | Latence p95 |", "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in res:
        lines.append(f"| {r['rate_target']:.0f} tx/s | {r['processed_tx_s']} tx/s | {r['batches']} | "
                     f"{r['batch_rows_median']} | {r['batch_ms_p50']} ms | {r['batch_ms_p95']} ms | "
                     f"{r['carousels_detected']} | {r['latency_s_p50']} s | {r['latency_s_p95']} s |")
    lines += ["\n![Latence](img/benchmark_latence.png)\n", "![Débit](img/benchmark_debit.png)\n"]
    with open(os.path.join(OUT_DIR, "benchmark.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
