"""Évaluation du système de détection contre la vérité terrain.

Mesures en ligne (lecture du lakehouse, sans Spark) :
  - rappel par typologie de fraude injectée (topic Kafka `ground-truth`) ;
  - précision des alertes de niveau >= MEDIUM ;
  - délai de détection (dernière transaction de la fraude -> alerte) ;
  - latence du chemin rapide (p50 / p95 / p99).

Étude d'ablation (option --ablation, Spark, à lancer pipeline arrêté) :
  motif seul -> + filtre temporel -> + score >= MEDIUM, sur les carrousels.

Usage (conteneur) :
    docker compose run --rm --no-deps spark-streaming python scripts/evaluate.py
    docker compose run --rm --no-deps spark-streaming python scripts/evaluate.py --ablation --minutes 30
Sorties : docs/resultats.md, docs/resultats.json, docs/img/*.png
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

from fraud import dashboard_data as data  # noqa: E402
from fraud.config import settings as S  # noqa: E402
from fraud.scoring import level_at_least  # noqa: E402

OUT_DIR = os.environ.get("DOCS_DIR", "docs")
TYPE_FR = {"CARROUSEL": "Carrousel (3 comptes)", "CYCLE_LONG": "Cycle long (4-5)", "FAN_IN_MULE": "Compte mule",
           "SCATTER_GATHER": "Scatter-gather", "PIVOT_HUB": "Pivot (3 carrousels)"}


# --------------------------------------------------------------------------- chargement
def load_ground_truth(timeout: float = 15.0) -> pd.DataFrame:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": S.kafka_bootstrap, "group.id": f"eval-{time.time()}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    meta = c.list_topics(S.topic_ground_truth, timeout=10).topics[S.topic_ground_truth]
    parts = [TopicPartition(S.topic_ground_truth, p, 0) for p in meta.partitions]
    c.assign(parts)
    ends = {p.partition: c.get_watermark_offsets(p, timeout=10)[1] for p in parts}
    rows, deadline = [], time.time() + timeout
    while time.time() < deadline and any(c.position([TopicPartition(S.topic_ground_truth, p)])[0].offset < e
                                         for p, e in ends.items() if e > 0):
        msg = c.poll(1.0)
        if msg is not None and not msg.error():
            rows.append(json.loads(msg.value()))
    c.close()
    df = pd.DataFrame(rows)
    if not df.empty:
        df["injected_at"] = pd.to_datetime(df["injected_at"], utc=True)
    return df


def tx_times(tx_ids: set[str]) -> pd.DataFrame:
    import pyarrow.dataset as ds
    df = data.read("transactions", ds.field("transaction_id").isin(list(tx_ids)),
                   ["transaction_id", "event_time", "kafka_timestamp"], days=2)
    for c in ("event_time", "kafka_timestamp"):
        df[c] = pd.to_datetime(df[c], utc=True)
    return df.set_index("transaction_id")


def matches(alert: pd.Series, gt: pd.Series) -> bool:
    """Une alerte couvre un scénario si elle partage au moins une transaction, ou si
    elle porte sur un compte clé (mule, pivot, source/destination) du scénario."""
    if set(alert["transaction_ids"]) & set(gt["transaction_ids"]):
        return True
    return alert["alert_type"] in ("MULE", "PIVOT", "SCATTER_GATHER", "CLUSTER") and \
        bool(set(alert["accounts"]) & set(gt["key_accounts"]))


# --------------------------------------------------------------------------- évaluation en ligne
def evaluate(settle_seconds: int = 120) -> dict:
    gt = load_ground_truth()
    if gt.empty:
        raise SystemExit("Aucune vérité terrain dans Kafka : lancez le producteur avec --fraud-every > 0.")
    start = gt["injected_at"].min() - pd.Timedelta(seconds=30)
    alerts = data.alerts(start)
    now = data.utcnow()
    # On n'évalue que les scénarios qui ont eu le temps d'être détectés.
    gt_all = gt.copy()  # précision : toute la vérité terrain (une alerte récente reste vraie)
    gt = gt[gt["injected_at"] <= now - pd.Timedelta(seconds=settle_seconds)].copy()  # rappel
    times = tx_times({t for ids in gt["transaction_ids"] for t in ids})
    gt["completed_at"] = gt["transaction_ids"].apply(
        lambda ids: times.loc[times.index.intersection(ids), "kafka_timestamp"].max())
    gt["ingested"] = gt["transaction_ids"].apply(lambda ids: len(times.index.intersection(ids)) == len(ids))

    sig = alerts[alerts["risk_level"].apply(lambda lvl: level_at_least(lvl, "MEDIUM"))] if not alerts.empty else alerts
    rows = []
    for _, g in gt.iterrows():
        hit = sig[sig.apply(lambda a, g=g: matches(a, g), axis=1)] if not sig.empty else sig
        expected = hit[hit["alert_type"].isin(g["expected_alert_types"])] if not hit.empty else hit
        # délai mesuré sur la typologie principale (ex. PIVOT pour un pivot, pas ses carrousels)
        primary = expected[expected["alert_type"] == g["expected_alert_types"][0]] if not expected.empty else expected
        first = (primary if not primary.empty else expected)["detected_at"].min() if not expected.empty else pd.NaT
        rows.append({"scenario_id": g["scenario_id"], "type": g["scenario_type"], "ingested": g["ingested"],
                     "detected": not expected.empty, "any_alert": not hit.empty,
                     "delay_s": (first - g["completed_at"]).total_seconds()
                     if pd.notna(first) and pd.notna(g["completed_at"]) else None,
                     "levels": ",".join(sorted(set(expected["risk_level"]))) if not expected.empty else ""})
    per = pd.DataFrame(rows)
    per = per[per["ingested"]]

    recall = (per.groupby("type").agg(injectes=("detected", "size"), detectes=("detected", "sum"),
                                      delai_median_s=("delay_s", "median"), delai_max_s=("delay_s", "max"))
              .reset_index())
    recall["rappel"] = recall["detectes"] / recall["injectes"]

    # Précision : alertes significatives émises pendant la période évaluée
    eval_alerts = sig[sig["detected_at"] <= now - pd.Timedelta(seconds=30)] if not sig.empty else sig
    eval_alerts = eval_alerts.assign(tp=eval_alerts.apply(
        lambda a: any(matches(a, g) for _, g in gt_all.iterrows()), axis=1)) if not eval_alerts.empty else eval_alerts
    precision = (eval_alerts.groupby("alert_type").agg(alertes=("tp", "size"), vraies=("tp", "sum")).reset_index()
                 if not eval_alerts.empty else pd.DataFrame(columns=["alert_type", "alertes", "vraies"]))
    precision["precision"] = precision["vraies"] / precision["alertes"]

    fast = alerts[(alerts["detector"] == "fast-path") & alerts["risk_level"].apply(
        lambda lvl: level_at_least(lvl, "MEDIUM"))]["latency_ms"].dropna() / 1000 if not alerts.empty else pd.Series()
    lat = {q: round(float(fast.quantile(q / 100)), 2) for q in (50, 95, 99)} if len(fast) else {}
    prog = data.jsonl_tail("pipeline_metrics.jsonl", 20000)
    fp = prog[prog["query"] == "ingest"] if not prog.empty else prog
    throughput = float(fp["processed_rps"].median()) if not fp.empty else None
    period = (now - start).total_seconds() / 60

    return {"generated_at": datetime.now(timezone.utc).isoformat(), "period_minutes": round(period, 1),
            "scenarios": int(len(per)), "alerts_total": int(len(alerts)), "alerts_medium_plus": int(len(eval_alerts)),
            "recall": recall.round(3).to_dict("records"), "precision": precision.round(3).to_dict("records"),
            "overall_recall": round(float(per["detected"].mean()), 3) if len(per) else None,
            "overall_precision": round(float(eval_alerts["tp"].mean()), 3) if len(eval_alerts) else None,
            "fast_latency_s": lat, "fast_processed_rps_median": throughput,
            "alerts_by_level": alerts["risk_level"].value_counts().to_dict() if not alerts.empty else {}}


# --------------------------------------------------------------------------- ablation (Spark)
def ablation(minutes: int) -> list[dict]:
    from pyspark.sql import functions as F

    from fraud import detection as D
    from fraud.scoring import score_cycle
    from fraud.spark_session import build_spark

    gt = load_ground_truth()
    carousel_tx = [set(ids) for t, ids in zip(gt["scenario_type"], gt["transaction_ids"])
                   if t in ("CARROUSEL", "PIVOT_HUB")]
    spark = build_spark("ablation")
    bronze = spark.read.format("delta").load(S.table("transactions"))
    mx = bronze.agg(F.max("event_time")).first()[0]
    lo = mx - pd.Timedelta(minutes=minutes)
    edges = D.to_edges(bronze.filter(F.col("event_time") >= F.lit(lo))).persist()
    n_edges = edges.count()
    results, seen = {}, {"motif": set(), "temporal": set(), "score": set()}
    step = pd.Timedelta(minutes=S.window_minutes)
    t = lo
    while t < mx:
        chunk = edges.filter((F.col("event_time") >= F.lit(t)) & (F.col("event_time") < F.lit(t + 2 * step)))
        chunk = chunk.localCheckpoint(eager=True)
        raw = D.find_cycles(chunk, 3, 10 ** 9, temporal=False).collect()
        seen["motif"].update(tuple(sorted(r[f"e{i}_transaction_id"] for i in (1, 2, 3))) for r in raw)
        for r in D.find_cycles(chunk, 3, S.window_seconds, temporal=True).collect():
            key = tuple(sorted(r[f"e{i}_transaction_id"] for i in (1, 2, 3)))
            seen["temporal"].add(key)
            es = sorted(D.rows_to_cycle_edges(r, 3), key=lambda e: e["event_time"])
            if level_at_least(score_cycle(es).level, "MEDIUM"):
                seen["score"].add(key)
        t += step
    in_range = [s for s in carousel_tx if s]
    for stage, cycles in seen.items():
        tp = sum(1 for c in cycles if any(set(c) <= s for s in in_range))
        found = sum(1 for s in in_range if any(set(c) <= s for c in cycles))
        results[stage] = {"cycles": len(cycles), "vrais": tp,
                          "precision": round(tp / len(cycles), 4) if cycles else None,
                          "carrousels_retrouves": found}
    spark.stop()
    label = {"motif": "Motif seul (a)->(b)->(c)->(a)", "temporal": "+ ordre temporel et durée ≤ 10 min",
             "score": "+ score ≥ MEDIUM (montants, vitesse…)"}
    return [{"etape": label[k], "aretes_analysees": n_edges, **v} for k, v in results.items()]


# --------------------------------------------------------------------------- rendu
def charts(res: dict) -> list[str]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    os.makedirs(os.path.join(OUT_DIR, "img"), exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "axes.spines.top": False, "axes.spines.right": False,
                         "axes.edgecolor": "#c3c2b7", "axes.labelcolor": "#52514e", "xtick.color": "#52514e",
                         "ytick.color": "#52514e"})
    files = []
    rec = pd.DataFrame(res["recall"])
    if not rec.empty:
        fig, ax = plt.subplots(figsize=(7, 3.2), dpi=150)
        labels = [TYPE_FR.get(t, t) for t in rec["type"]]
        ax.barh(labels, rec["rappel"] * 100, color="#2a78d6", height=0.55)
        for i, (r, n, d) in enumerate(zip(rec["rappel"], rec["injectes"], rec["detectes"])):
            ax.text(min(r * 100 + 1, 92), i, f"{r * 100:.0f} %  ({int(d)}/{int(n)})", va="center",
                    color="#0b0b0b", fontsize=9)
        ax.set_xlim(0, 110)
        ax.set_xlabel("Rappel (%)")
        ax.set_title("Rappel par typologie de fraude injectée", loc="left", fontsize=11)
        ax.invert_yaxis()
        fig.tight_layout()
        path = os.path.join(OUT_DIR, "img", "rappel_par_typologie.png")
        fig.savefig(path)
        files.append(path)
        plt.close(fig)
    alerts = data.alerts(data.utcnow() - pd.Timedelta(minutes=res["period_minutes"] + 1))
    lat = alerts[alerts["detector"] == "fast-path"]["latency_ms"].dropna() / 1000 if not alerts.empty else []
    if len(lat):
        fig, ax = plt.subplots(figsize=(7, 3), dpi=150)
        ax.hist(lat, bins=30, color="#2a78d6", edgecolor="#fcfcfb", linewidth=1)
        p50, p95 = lat.median(), lat.quantile(0.95)
        for v, name in ((p50, "p50"), (p95, "p95")):
            ax.axvline(v, color="#52514e", linewidth=1, linestyle="--")
            ax.text(v, ax.get_ylim()[1] * 0.92, f" {name} = {v:.1f} s", color="#0b0b0b", fontsize=9)
        ax.set_xlabel("Latence ingestion Kafka → alerte (s)")
        ax.set_ylabel("Alertes")
        ax.set_title("Latence du chemin rapide (cycles)", loc="left", fontsize=11)
        fig.tight_layout()
        path = os.path.join(OUT_DIR, "img", "latence_chemin_rapide.png")
        fig.savefig(path)
        files.append(path)
        plt.close(fig)
    return files


def markdown(res: dict, abl: list[dict] | None) -> str:
    lines = ["# Résultats de l'évaluation\n",
             f"_Généré le {res['generated_at'][:19].replace('T', ' ')} UTC — période de {res['period_minutes']} min, "
             f"{res['scenarios']} fraudes injectées, {res['alerts_total']} alertes (dont "
             f"{res['alerts_medium_plus']} de niveau ≥ MEDIUM)._\n",
             "## Rappel par typologie\n", "| Typologie | Injectées | Détectées | Rappel | Délai médian | Délai max |",
             "|---|---:|---:|---:|---:|---:|"]
    for r in res["recall"]:
        med = r["delai_median_s"] if r["delai_median_s"] is not None else "—"
        mx = r["delai_max_s"] if r["delai_max_s"] is not None else "—"
        lines.append(f"| {TYPE_FR.get(r['type'], r['type'])} | {r['injectes']} | {r['detectes']} | "
                     f"{100 * r['rappel']:.0f} % | {med} s | {mx} s |")
    lines += [f"\n**Rappel global : {100 * (res['overall_recall'] or 0):.1f} %**\n",
              "## Précision des alertes (niveau ≥ MEDIUM)\n", "| Type d'alerte | Alertes | Vraies | Précision |",
              "|---|---:|---:|---:|"]
    for r in res["precision"]:
        lines.append(f"| {r['alert_type']} | {r['alertes']} | {r['vraies']} | {100 * r['precision']:.1f} % |")
    lines += [f"\n**Précision globale : {100 * (res['overall_precision'] or 0):.1f} %**\n",
              "## Latence et débit\n",
              f"- Latence chemin rapide (ingestion Kafka → alerte) : p50 **{res['fast_latency_s'].get(50, '—')} s**, "
              f"p95 **{res['fast_latency_s'].get(95, '—')} s**, p99 {res['fast_latency_s'].get(99, '—')} s",
              f"- Débit de traitement médian du chemin rapide : {res['fast_processed_rps_median'] or 0:.0f} tx/s",
              f"- Répartition des alertes par niveau : {res['alerts_by_level']}\n"]
    if abl:
        lines += ["## Étude d'ablation (carrousels)\n",
                  "| Étape | Cycles retenus | Vrais carrousels | Précision | Carrousels retrouvés |",
                  "|---|---:|---:|---:|---:|"]
        for a in abl:
            prec = f"{100 * a['precision']:.2f} %" if a["precision"] is not None else "—"
            lines.append(f"| {a['etape']} | {a['cycles']} | {a['vrais']} | {prec} | {a['carrousels_retrouves']} |")
        lines.append(f"\n_{abl[0]['aretes_analysees']} transactions analysées._\n")
    lines += ["## Graphiques\n", "![Rappel](img/rappel_par_typologie.png)\n",
              "![Latence](img/latence_chemin_rapide.png)\n"]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ablation", action="store_true")
    ap.add_argument("--minutes", type=int, default=30, help="période analysée par l'ablation")
    ap.add_argument("--settle", type=int, default=480,
                    help="délai laissé à la détection (s) : durée d'un scénario + cadence du chemin lent")
    args = ap.parse_args()
    res = evaluate(args.settle)
    abl = ablation(args.minutes) if args.ablation else None
    if abl:
        res["ablation"] = abl
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "resultats.json"), "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2, default=str)
    charts(res)
    md = markdown(res, abl)
    with open(os.path.join(OUT_DIR, "resultats.md"), "w", encoding="utf-8") as f:
        f.write(md)
    print(md)


if __name__ == "__main__":
    main()
