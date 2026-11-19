"""Exporte les données des runs pour les figures du rapport.

À lancer dans le conteneur (accès au lakehouse et à Kafka) :
    docker compose run --rm --no-deps -v "<projet>/rapport:/app/rapport" -v "<projet>/docs:/app/docs" \
        dashboard python rapport/exporter_donnees.py

Écrit dans rapport/donnees/ des CSV/JSON lus ensuite par generer_figures.py (hôte).
Fenêtres utilisées :
  - run d'évaluation final : de la première transaction du bronze à docs/resultats.json ;
  - benchmark : docs/benchmark.json ;
  - test de panne : rapport/donnees/panne_*.txt.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
from collections import defaultdict
from datetime import timedelta

sys.path.insert(0, "/app")

import networkx as nx  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402

from fraud import dashboard_data as data  # noqa: E402
from fraud.config import TABLES  # noqa: E402
from fraud.config import settings as S  # noqa: E402
from fraud.graph_index import SlidingGraphIndex  # noqa: E402
from scripts.evaluate import load_ground_truth, matches  # noqa: E402

OUT = "/app/rapport/donnees"
os.makedirs(OUT, exist_ok=True)


def ecrire(df: pd.DataFrame, nom: str) -> None:
    df.to_csv(os.path.join(OUT, nom), index=False)
    print(f"{nom:<32} {len(df):>8} lignes")


def lire_table(cle: str) -> pd.DataFrame:
    t = data.delta(cle)
    return t.to_pyarrow_dataset().to_table().to_pandas() if t is not None else pd.DataFrame()


# --------------------------------------------------------------------------- fenêtres
res = json.load(open("/app/docs/resultats.json", encoding="utf-8"))
bench = json.load(open("/app/docs/benchmark.json", encoding="utf-8"))
bronze = lire_table("transactions")
for c in ("event_time", "kafka_timestamp", "ingested_at"):
    bronze[c] = pd.to_datetime(bronze[c], utc=True)
fin_run = pd.Timestamp(res["generated_at"])
debut_run = bronze["kafka_timestamp"].min()
fenetres = {"run_debut": debut_run.isoformat(), "run_fin": fin_run.isoformat(),
            "bench_fin": bench["generated_at"]}
for f in ("debut", "kill", "restart", "fin"):
    p = os.path.join(OUT, f"panne_{f}.txt")
    if os.path.exists(p):
        fenetres[f"panne_{f}"] = open(p).read().strip()
json.dump(fenetres, open(os.path.join(OUT, "fenetres.json"), "w"), indent=2)
run = bronze[(bronze["kafka_timestamp"] >= debut_run) & (bronze["kafka_timestamp"] <= fin_run)].copy()

# --------------------------------------------------------------------------- trafic
b = bronze.assign(minute=bronze["kafka_timestamp"].dt.floor("min"),
                  gros=(bronze["montant"] >= S.high_value_mad).astype(int))
ecrire(b.groupby("minute").agg(tx=("transaction_id", "size"), montant=("montant", "sum"),
                               gros_flux=("gros", "sum")).reset_index(), "trafic_minute.csv")
ecrire(run.groupby("type_operation")["montant"].describe(percentiles=[.05, .25, .5, .75, .95]).reset_index(),
       "montants_par_type.csv")
echant = run.sample(n=min(40_000, len(run)), random_state=42)
ecrire(echant[["type_operation", "montant"]], "montants_echantillon.csv")
deg = pd.DataFrame({
    "emises": run.groupby("source_account").size(),
    "recues": run.groupby("target_account").size()}).fillna(0).astype(int).reset_index(names="compte")
ecrire(deg, "activite_comptes.csv")
lag = (run["ingested_at"] - run["kafka_timestamp"]).dt.total_seconds()
retard = (run["kafka_timestamp"] - run["event_time"]).dt.total_seconds()
ecrire(pd.DataFrame({"retard_evenement_s": retard}), "retards_evenements.csv")
print("délai ingestion médian (s):", round(lag.median(), 2))

# --------------------------------------------------------------------------- vérité terrain & alertes
gt = load_ground_truth()
gt.to_json(os.path.join(OUT, "verite_terrain.json"), orient="records", date_format="iso", force_ascii=False)
alertes = data.alerts(debut_run - timedelta(hours=1))
al = alertes.copy()
al["features"] = al["features"].apply(lambda f: json.dumps(f, ensure_ascii=False))
al["accounts"] = al["accounts"].apply(lambda a: ";".join(a))
al["transaction_ids"] = al["transaction_ids"].apply(lambda a: ";".join(a))
ecrire(al, "alertes.csv")
for cle in ("subgraph_vertices", "subgraph_edges", "quarantine", "traffic_windows", "account_metrics"):
    ecrire(lire_table(cle), f"{TABLES[cle].split('/')[-1]}.csv")

# --------------------------------------------------------------------------- évaluation par scénario
gt_run = gt[(gt["injected_at"] >= debut_run) & (gt["injected_at"] <= fin_run - timedelta(seconds=480))]
tx_idx = bronze.set_index("transaction_id")
lignes = []
for _, g in gt_run.iterrows():
    sig = alertes[alertes["risk_level"] != "LOW"]
    hit = sig[sig.apply(lambda a, g=g: matches(a, g), axis=1)]
    exp = hit[hit["alert_type"].isin(g["expected_alert_types"])]
    prim = exp[exp["alert_type"] == g["expected_alert_types"][0]]
    t = tx_idx.loc[tx_idx.index.intersection(g["transaction_ids"])]
    fin_sc = t["kafka_timestamp"].max() if len(t) else pd.NaT
    premier = (prim if len(prim) else exp)["detected_at"].min() if len(exp) else pd.NaT
    lignes.append({"scenario_id": g["scenario_id"], "type": g["scenario_type"], "injecte": g["injected_at"],
                   "duree_s": (fin_sc - t["kafka_timestamp"].min()).total_seconds() if len(t) else None,
                   "detecte": len(exp) > 0,
                   "delai_s": (premier - fin_sc).total_seconds() if pd.notna(premier) and pd.notna(fin_sc) else None,
                   "nb_alertes": len(hit), "montant": g["total_amount"], "nb_tx": len(g["transaction_ids"])})
ecrire(pd.DataFrame(lignes), "evaluation_scenarios.csv")

# --------------------------------------------------------------------------- vitrine : une alerte par type
vitrine = {}
run_al = alertes[(alertes["detected_at"] >= debut_run) & (alertes["detected_at"] <= fin_run)]
for typ in ("CYCLE", "CYCLE_LONG", "MULE", "SCATTER_GATHER", "PIVOT", "CLUSTER"):
    c = run_al[run_al["alert_type"] == typ].sort_values(["risk_score", "detected_at"], ascending=[False, True])
    if c.empty:
        continue
    a = c.iloc[0]
    v, e = data.subgraphs([a["alert_id"]])
    vitrine[typ] = {"alerte": {k: (str(a[k]) if not isinstance(a[k], (list, dict, int, float)) else a[k])
                               for k in ("alert_id", "risk_score", "risk_level", "explanation", "total_amount",
                                         "latency_ms", "detected_at", "accounts", "features")},
                    "sommets": v.drop(columns=["dt"], errors="ignore").to_dict("records"),
                    "aretes": e.assign(event_time=e["event_time"].astype(str)).drop(
                        columns=["dt"], errors="ignore").to_dict("records")}
json.dump(vitrine, open(os.path.join(OUT, "vitrine_alertes.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1, default=str)

# Fiche d'investigation PDF de l'alerte pivot (ou carrousel) de la vitrine
try:
    from fraud.report_pdf import investigation_pdf
    typ = "PIVOT" if "PIVOT" in vitrine else "CYCLE"
    aid = vitrine[typ]["alerte"]["alert_id"]
    row = alertes[alertes["alert_id"] == aid].iloc[0].to_dict()
    v, e = data.subgraphs([aid])
    with open("/app/rapport/figures/fiche_investigation.pdf", "wb") as f:
        f.write(investigation_pdf(row, v, e))
    print("fiche_investigation.pdf écrite")
except Exception as exc:
    print("fiche PDF impossible :", exc)

# --------------------------------------------------------------------------- mule vs commerçant
# mule détectée pendant le run, de score maximal et à la rafale la plus nette
alr = al[(al["alert_type"] == "MULE") & (al["detected_at"] >= debut_run + timedelta(minutes=28))
         & (al["detected_at"] <= fin_run)]  # 20 min d'historique visibles avant la rafale
alr = alr.sort_values(["risk_score", "detected_at"], ascending=[False, True])
mules_detectees = [a.split(";")[0] for a in alr["accounts"]]
mule = next((m for m in mules_detectees if (run["target_account"] == m).sum() >= 10), None)
recus = run.groupby("target_account").size().sort_values(ascending=False)
marchand = recus.index[0]
vel = {}
for nom, acc in (("mule", mule), ("commercant", marchand)):
    if acc is None:
        continue
    dep = run[run["target_account"] == acc][["kafka_timestamp", "montant", "source_account"]]
    out = run[run["source_account"] == acc][["kafka_timestamp", "montant"]]
    vel[nom] = {"compte": acc,
                "depots": [{"t": t.isoformat(), "montant": m} for t, m in zip(dep["kafka_timestamp"], dep["montant"])],
                "sorties": [{"t": t.isoformat(), "montant": m} for t, m in zip(out["kafka_timestamp"], out["montant"])]}
json.dump(vel, open(os.path.join(OUT, "mule_vs_commercant.json"), "w"), indent=1)

# --------------------------------------------------------------------------- élagage des candidats (rejeu)
idx = SlidingGraphIndex(timedelta(minutes=S.window_minutes + S.watermark_minutes))
r = run.sort_values("kafka_timestamp")
r = r.assign(lot=r["kafka_timestamp"].dt.floor("2s"))
mes = []
for k, (lot, grp) in enumerate(r.groupby("lot")):
    fresh = [{"transaction_id": t, "src": s, "dst": d, "montant": m, "event_time": et, "type_operation": o,
              "kafka_timestamp": kt}
             for t, s, d, m, et, o, kt in zip(grp["transaction_id"], grp["source_account"], grp["target_account"],
                                             grp["montant"], grp["event_time"], grp["type_operation"],
                                             grp["kafka_timestamp"])]
    idx.add(fresh)
    if k % 10 == 0:
        idx.evict()
    cand = idx.candidates([e["transaction_id"] for e in fresh])
    mes.append({"lot": lot, "nouvelles": len(fresh), "fenetre": len(idx), "candidates": len(cand)})
ecrire(pd.DataFrame(mes), "elagage_candidats.csv")

# --------------------------------------------------------------------------- élagage du graphe des gros flux
fun = []
t0 = run["event_time"].min() + timedelta(minutes=10)
while t0 <= run["event_time"].max():
    w = run[(run["event_time"] > t0 - timedelta(minutes=10)) & (run["event_time"] <= t0)]
    hv = w[w["montant"] >= S.high_value_mad][["source_account", "target_account"]].drop_duplicates()
    core = hv
    for _ in range(4):
        both = set(core["source_account"]) & set(core["target_account"])
        core = core[core["source_account"].isin(both) & core["target_account"].isin(both)]
    g = nx.DiGraph(list(zip(core["source_account"], core["target_account"])))
    tailles = [len(c) for c in nx.strongly_connected_components(g)] if len(core) else []
    fun.append({"t": t0, "aretes_fenetre": len(w), "aretes_gros_flux": len(hv), "aretes_noyau": len(core),
                "scc_3plus": sum(1 for x in tailles if x >= 3), "scc_4plus": sum(1 for x in tailles if x >= 4)})
    t0 += timedelta(minutes=2)
ecrire(pd.DataFrame(fun), "elagage_gros_flux.csv")

# --------------------------------------------------------------------------- Delta : historique & partitions
hist, parts = [], []
for cle, chemin in TABLES.items():
    h = data.history(cle, 5000)
    if not h.empty:
        hist.append(h.assign(table=chemin)[["table", "version", "timestamp", "operation"]])
    p = data.partition_layout(cle)
    if not p.empty:
        parts.append(p.assign(table=chemin))
ecrire(pd.concat(hist), "delta_historique.csv")
ecrire(pd.concat(parts), "delta_partitions.csv")
print("versions par table :", {t: int(g["version"].max()) for t, g in pd.concat(hist).groupby("table")})
dt_bronze = data.delta("transactions")
nb_fichiers = len(pa.record_batch(dt_bronze.get_add_actions(flatten=True)).to_pandas())
json.dump({"fichiers_bronze_actifs": nb_fichiers, "lignes_bronze": len(bronze), "lignes_run": len(run)},
          open(os.path.join(OUT, "volumes.json"), "w"), indent=2)

# --------------------------------------------------------------------------- métriques des runs
for nom in ("fast_path.jsonl", "slow_path.jsonl", "pipeline_metrics.jsonl"):
    src = os.path.join(S.metrics_dir, nom)
    if os.path.exists(src):
        shutil.copy(src, os.path.join(OUT, nom))
for nom in ("resultats.json", "benchmark.json", "ablation.json"):
    shutil.copy(os.path.join("/app/docs", nom), os.path.join(OUT, nom))
print("export terminé")
