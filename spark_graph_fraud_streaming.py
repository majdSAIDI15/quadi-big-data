"""Livrable 2 — Spark Structured Streaming + GraphFrames : détection de fraude en graphe.

Architecture « à deux vitesses » dans une seule application Spark (3 requêtes) :

  ingest      Kafka -> foreachBatch (déclencheur 1 s) :
                1. contrôle qualité : messages invalides -> quarantaine Delta ;
                2. watermark 2 min (événements trop tardifs écartés) + dédoublonnage ;
                3. graphe glissant 10 min mis à jour de façon incrémentale ;
                4. motif (a)->(b)->(c)->(a) incrémental (GraphFrames) ;
                5. score + alerte Kafka `fraud-alerts` (chemin critique de latence) ;
                6. bronze Delta + gold Delta (écritures idempotentes).
  slow-path   toutes les 90 s sur la fenêtre 10 min lue dans le bronze :
              PageRank, composantes fortement connexes (SCC), cycles longs (4-5),
              comptes mules, scatter-gather, comptes pivots, Connected Components
              (clusters), instantané des métriques de compte.
  traffic     agrégation native en fenêtre glissante window(10 min, 1 min) + withWatermark(2 min).

Tolérance aux pannes : checkpoints Spark, écritures Delta idempotentes
(txnAppId/txnVersion), alert_id déterministe, reconstruction du graphe glissant
depuis le bronze au redémarrage.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
import traceback
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from fraud import detection as D
from fraud.alerts import Alert, make_alert
from fraud.config import settings
from fraud.graph_index import DepositLog, SlidingGraphIndex, velocity_spike
from fraud.scoring import (
    explain_cluster,
    explain_cycle,
    explain_mule,
    explain_pivot,
    explain_scatter_gather,
    level_at_least,
    score_cluster,
    score_cycle,
    score_mule,
    score_pivot,
    score_scatter_gather,
)
from fraud.spark_session import build_spark
from fraud_graph_delta_schema import DEFS, ensure_schema

S = settings
UTC = timezone.utc
TXN_JSON_SCHEMA = ("transaction_id STRING, source_account STRING, target_account STRING, montant DOUBLE, "
                   "devise STRING, type_operation STRING, timestamp STRING")
EDGE_SCHEMA = ("transaction_id STRING, src STRING, dst STRING, montant DOUBLE, event_time TIMESTAMP, "
               "type_operation STRING, kafka_timestamp TIMESTAMP")
BRONZE_COLS = DEFS["transactions"].names


def utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def now_utc() -> datetime:
    return datetime.now(UTC)


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)


def ms(t0: float) -> int:
    return int(1000 * (time.time() - t0))


# --------------------------------------------------------------------------- lecture & parsing
def read_kafka(spark, max_offsets: int | None = None) -> DataFrame:
    r = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", S.kafka_bootstrap)
         .option("subscribe", S.topic_transactions)
         .option("startingOffsets", os.environ.get("STARTING_OFFSETS", "latest"))
         .option("failOnDataLoss", "false"))
    if max_offsets:
        r = r.option("maxOffsetsPerTrigger", max_offsets)
    return r.load()


def parse(kafka_df: DataFrame) -> DataFrame:
    """JSON Kafka -> colonnes typées + colonne `error` (contrôle qualité)."""
    df = (kafka_df.select(F.col("value").cast("string").alias("raw_value"),
                          F.col("partition").alias("kafka_partition"),
                          F.col("offset").alias("kafka_offset"),
                          F.col("timestamp").alias("kafka_timestamp"))
          .withColumn("j", F.from_json("raw_value", TXN_JSON_SCHEMA))
          .select("*", "j.*").drop("j")
          .withColumn("event_time", F.to_timestamp("timestamp")))
    error = (F.when(F.col("transaction_id").isNull() | F.col("source_account").isNull()
                    | F.col("target_account").isNull() | F.col("montant").isNull(),
                    "champ manquant ou JSON invalide")
             .when(F.col("event_time").isNull(), "timestamp invalide")
             .when(F.col("montant") <= 0, "montant non positif")
             .when(F.col("source_account") == F.col("target_account"), "auto-virement"))
    return df.withColumn("error", error)


# --------------------------------------------------------------------------- métriques
class JsonlWriter:
    def __init__(self, path: str, max_bytes: int = 50_000_000):
        self.path = path
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.exists(path) and os.path.getsize(path) > max_bytes:
            os.replace(path, path + ".old")

    def write(self, record: dict) -> None:
        line = json.dumps(record, default=str, ensure_ascii=False)
        with self.lock, open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


class MetricsListener(StreamingQueryListener):
    """Progression de chaque requête -> pipeline_metrics.jsonl (page « Santé » du dashboard)."""

    def __init__(self, writer: JsonlWriter):
        self.writer = writer

    def onQueryStarted(self, event):
        log(f"[listener] requête démarrée : {event.name}")

    def onQueryProgress(self, event):
        try:
            p = json.loads(event.progress.json)
            ops = p.get("stateOperators") or []
            self.writer.write({
                "ts": p.get("timestamp"), "query": p.get("name"), "batch_id": p.get("batchId"),
                "num_input_rows": p.get("numInputRows"),
                "input_rps": p.get("inputRowsPerSecond"), "processed_rps": p.get("processedRowsPerSecond"),
                "trigger_ms": (p.get("durationMs") or {}).get("triggerExecution"),
                "add_batch_ms": (p.get("durationMs") or {}).get("addBatch"),
                "watermark": (p.get("eventTime") or {}).get("watermark"),
                "dropped_by_watermark": sum(o.get("numRowsDroppedByWatermark", 0) for o in ops),
                "state_rows": sum(o.get("numRowsTotal", 0) for o in ops),
            })
        except Exception as exc:  # le listener ne doit jamais faire tomber le pipeline
            log(f"[listener] erreur : {exc}")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log(f"[listener] requête terminée : {event.id} {event.exception or ''}")


# --------------------------------------------------------------------------- pipeline
class FraudGraphPipeline:
    def __init__(self, spark):
        self.spark = spark
        # Graphe glissant du chemin rapide : index d'adjacence (fenêtre 10 min + watermark 2 min)
        self.index = SlidingGraphIndex(timedelta(minutes=S.window_minutes + S.watermark_minutes))
        self.batches_since_evict = 0
        # Rythme habituel de chaque compte (dépôts des 40 dernières minutes) pour les mules
        self.deposits = DepositLog()
        self.pagerank_pct: dict[str, float] = {}
        self.emitted: set[str] = set()
        self.slow_batches = 0
        self.kafka = None
        self.app_ids: dict[str, str] = {}
        self.fast_log = JsonlWriter(os.path.join(S.metrics_dir, "fast_path.jsonl"))
        self.slow_log = JsonlWriter(os.path.join(S.metrics_dir, "slow_path.jsonl"))
        self.progress_log = JsonlWriter(os.path.join(S.metrics_dir, "pipeline_metrics.jsonl"))

    # -- utilitaires ------------------------------------------------------------
    def ckpt(self, query: str) -> str:
        return os.path.join(S.checkpoint_dir, query)

    def app_id(self, query: str) -> str:
        """Identifiant d'écriture idempotente Delta, lié à la vie du checkpoint :
        si le checkpoint est supprimé, un nouvel id est généré (sinon les numéros
        de batch repartant à 0 seraient ignorés par Delta)."""
        id_file = os.path.join(S.checkpoint_dir, "_txn", f"{query}.id")
        has_ckpt = os.path.isdir(os.path.join(self.ckpt(query), "offsets"))
        if has_ckpt and os.path.exists(id_file):
            with open(id_file, encoding="utf-8") as f:
                return f.read().strip()
        os.makedirs(os.path.dirname(id_file), exist_ok=True)
        app = f"{query}-{uuid.uuid4().hex[:12]}"
        with open(id_file, "w", encoding="utf-8") as f:
            f.write(app)
        return app

    def delta(self, key: str) -> DataFrame:
        return self.spark.read.format("delta").load(S.table(key))

    def recent_dt(self, ts: datetime | None = None) -> list[str]:
        ts = ts or now_utc()
        return [(ts - timedelta(days=d)).strftime("%Y-%m-%d") for d in (1, 0)]

    def _write(self, key: str, rows: list[dict], app_id: str, batch_id: int) -> None:
        """Écriture Delta idempotente d'un petit lot de lignes construit sur le driver (Arrow)."""
        if not rows:
            return
        d = DEFS[key]
        pdf = pd.DataFrame([{c: r.get(c) for c in d.names} for r in rows], columns=d.names, dtype=object)
        (self.spark.createDataFrame(pdf, schema=d.spark_schema()).coalesce(1)  # 1 tâche, 1 fichier
         .write.format("delta").mode("append")
         .option("txnAppId", app_id).option("txnVersion", batch_id)
         .save(S.table(key)))

    def publish(self, alerts: list[Alert]) -> None:
        """Alerte Kafka d'abord (chemin critique de latence), Delta ensuite."""
        now = now_utc()
        for a in alerts:
            ingest = [utc(e.get("kafka_timestamp")) for e in a.edges if e.get("kafka_timestamp")]
            a.detected_at = now
            a.latency_ms = int((now - max(ingest)).total_seconds() * 1000) if ingest else None
            if level_at_least(a.risk_level, S.kafka_min_level):
                self.kafka.produce(S.topic_alerts, key=a.alert_id, value=a.kafka_value())
        if alerts:
            self.kafka.flush(5)

    def write_alerts(self, alerts: list[Alert], app_id: str, detector: str, batch_id: int) -> None:
        if not alerts:
            return
        self._write("fraud_alerts", [dict(a.row(), detector=detector) for a in alerts], app_id, batch_id)
        self._write("subgraph_vertices", [r for a in alerts for r in a.vertex_rows(self.pagerank_pct)],
                    app_id, batch_id)
        self._write("subgraph_edges", [r for a in alerts for r in a.edge_rows()], app_id, batch_id)

    @staticmethod
    def normalize(edges: list[dict]) -> list[dict]:
        for e in edges:
            e["event_time"] = utc(e["event_time"])
            e["kafka_timestamp"] = utc(e.get("kafka_timestamp"))
        return sorted(edges, key=lambda e: (e["event_time"], e["transaction_id"]))

    @staticmethod
    def edge_dicts(pdf: pd.DataFrame) -> list[dict]:
        def ts(x):
            return utc(pd.Timestamp(x).to_pydatetime()) if pd.notna(x) else None

        return [{"transaction_id": r.transaction_id, "src": r.src, "dst": r.dst, "montant": float(r.montant),
                 "event_time": ts(r.event_time), "type_operation": r.type_operation,
                 "kafka_timestamp": ts(r.kafka_timestamp)}
                for r in pdf.itertuples(index=False)]

    # -- reprise après redémarrage ------------------------------------------------
    def recover(self) -> None:
        """Reconstruit le graphe glissant depuis le bronze (12 dernières minutes)."""
        bronze = self.delta("transactions").filter(F.col("dt").isin(self.recent_dt()))
        mx = utc(bronze.agg(F.max("event_time")).first()[0])
        if mx:
            pdf = D.to_edges(bronze.filter(F.col("event_time") >= F.lit(mx - self.deposits.retention))).toPandas()
            edges = self.edge_dicts(pdf)
            self.deposits.add(edges)
            self.index.add([e for e in edges if e["event_time"] >= mx - self.index.retention])
        alerts = self.delta("fraud_alerts").filter(F.col("dt").isin(self.recent_dt()))
        self.emitted = {r.alert_id for r in alerts.select("alert_id").collect()}
        snap = self.delta("account_metrics").filter(F.col("dt").isin(self.recent_dt()))
        last = snap.agg(F.max("snapshot_time")).first()[0]
        if last is not None:
            rows = snap.filter(F.col("snapshot_time") == last).select("account_id", "pagerank_pct").collect()
            self.pagerank_pct = {r.account_id: r.pagerank_pct for r in rows if r.pagerank_pct is not None}
        log(f"[recover] graphe glissant={len(self.index)} arêtes, alertes connues={len(self.emitted)}, "
            f"pagerank={len(self.pagerank_pct)} comptes, max_event_time={self.index.max_time}")

    # -- chemin rapide : ingestion + cycles en quasi temps réel ----------------------
    def ingest(self, batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        app = self.app_ids["ingest"]
        batch = batch_df.persist()
        wm = self.index.max_time - timedelta(minutes=S.watermark_minutes) if self.index.max_time else None
        # Un seul passage sur le lot, ramené au driver via Arrow (quelques milliers de lignes)
        pdf = batch.select("transaction_id", F.col("source_account").alias("src"),
                           F.col("target_account").alias("dst"), "montant", "event_time", "type_operation",
                           "kafka_timestamp", F.col("error").isNull().alias("ok")).toPandas()
        if pdf.empty:
            batch.unpersist()
            return
        n_invalid = int((~pdf["ok"]).sum())
        fresh, seen, replayed, n_late, n_dup = [], set(), [], 0, 0
        for e in self.edge_dicts(pdf[pdf["ok"]]):
            if wm and e["event_time"] < wm:
                n_late += 1                       # au-delà du watermark de 2 min : écarté
            elif e["transaction_id"] in self.index:
                replayed.append(e["transaction_id"])  # déjà vu (doublon Kafka ou rejeu)
            elif e["transaction_id"] in seen:
                n_dup += 1                        # doublon dans le lot
            else:
                seen.add(e["transaction_id"])
                fresh.append(e)
        t_quality = t_window = t_detect = t_publish = time.time()

        # 1) contrôle qualité -> quarantaine
        if n_invalid:
            (batch.filter(F.col("error").isNotNull()).coalesce(1).withColumn("ingested_at", F.current_timestamp())
             .withColumn("dt", F.date_format("kafka_timestamp", "yyyy-MM-dd"))
             .select(*DEFS["quarantine"].names).write.format("delta").mode("append")
             .option("txnAppId", app).option("txnVersion", batch_id).save(S.table("quarantine")))

        rows, alert_list = [], []
        if fresh:
            # 2) graphe glissant : ajout incrémental + purge périodique
            self.index.add(fresh)
            self.deposits.add(fresh)
            self.batches_since_evict += 1
            if self.batches_since_evict >= 10:
                self.index.evict()
                self.deposits.evict()
                self.batches_since_evict = 0
            t_window = time.time()

            # 3) motif finding incrémental : élagage O(lot) dans l'index, puis motif GraphFrames
            #    (a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a) sur le sous-graphe candidat
            fresh_ids = {e["transaction_id"] for e in fresh}
            ids = self.index.candidates(list(fresh_ids))
            if len(ids) >= 3:
                cand = D.frame_from_rows(self.spark, self.index.rows(ids), with_is_new=fresh_ids)
                rows = D.find_cycles(cand, 3, S.window_seconds, temporal=True, only_new=True).collect()
                D.release(cand)
            t_detect = time.time()

            # 4) score + alertes Kafka (chemin critique)
            alerts: dict[str, Alert] = {}
            for r in rows:
                edges = self.normalize(D.rows_to_cycle_edges(r, 3))
                sc = score_cycle(edges, self.pagerank_pct)
                if sc.level is None:
                    continue
                roles = {e["src"]: "RELAIS" for e in edges}
                roles[edges[0]["src"]] = "ORIGINE"
                a = make_alert("CYCLE", [e["transaction_id"] for e in edges], sc, [e["src"] for e in edges],
                               edges, explain_cycle(edges, sc), now_utc(), roles, cycle_length=3)
                alerts[a.alert_id] = a
            alert_list = list(alerts.values())
            self.publish(alert_list)
            t_publish = time.time()

            # 5) persistance idempotente : gold puis bronze (en cas de panne entre les deux,
            #    le lot rejoué est redétecté et les écritures déjà faites sont ignorées)
            self.write_alerts(alert_list, app, "fast-path", batch_id)
            bronze = batch.filter(F.col("error").isNull())
            if wm:
                bronze = bronze.filter(F.col("event_time") >= F.lit(wm))
            if replayed:
                bronze = bronze.filter(~F.col("transaction_id").isin(replayed))
            (bronze.dropDuplicates(["transaction_id"]).coalesce(1)
             .withColumn("ingested_at", F.current_timestamp())
             .withColumn("dt", F.date_format("event_time", "yyyy-MM-dd")).select(*BRONZE_COLS)
             .write.format("delta").mode("append")
             .option("txnAppId", app).option("txnVersion", batch_id).save(S.table("transactions")))
        batch.unpersist()
        n_fresh = len(fresh)

        self.fast_log.write({
            "ts": now_utc().isoformat(), "batch_id": batch_id, "rows": len(pdf), "invalid": n_invalid,
            "late": n_late, "duplicates": n_dup + len(replayed), "fresh": n_fresh,
            "graph_edges": len(self.index),
            "watermark": wm.isoformat() if wm else None,
            "cycles_found": len(rows), "alerts": len(alert_list),
            "alerts_kafka": sum(level_at_least(a.risk_level, S.kafka_min_level) for a in alert_list),
            "quality_ms": int(1000 * (t_quality - t0)), "window_ms": int(1000 * (t_window - t_quality)),
            "detect_ms": int(1000 * (t_detect - t_window)), "publish_ms": int(1000 * (t_publish - t_detect)),
            "persist_ms": ms(t_publish), "total_ms": ms(t0),
            "alert_latency_ms": [a.latency_ms for a in alert_list],
        })
        for a in alert_list:
            if level_at_least(a.risk_level, "MEDIUM"):
                log(f"[ALERTE {a.risk_level:<8}] {a.alert_type} score={a.risk_score} "
                    f"latence={a.latency_ms} ms — {a.explanation[:140]}")

    # -- chemin lent : analyses globales du graphe --------------------------------
    def slow_path(self, _batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        app = self.app_ids["slow-path"]
        bronze = self.delta("transactions").filter(F.col("dt").isin(self.recent_dt()))
        mx = utc(bronze.agg(F.max("event_time")).first()[0])
        if mx is None:
            return
        day = mx.strftime("%Y-%m-%d")
        # Parallélisme volontairement réduit (1-2 partitions) : le chemin lent ne doit
        # pas monopoliser les cœurs dont le chemin rapide a besoin pour la latence.
        win = (D.to_edges(bronze.filter(F.col("event_time") >= F.lit(mx - timedelta(seconds=S.window_seconds))))
               .coalesce(2).localCheckpoint(eager=True))
        n_edges = win.count()
        hv = win.filter(F.col("montant") >= S.high_value_mad).coalesce(1).localCheckpoint(eager=True)
        timings: dict[str, int] = {}
        detected = now_utc()
        alerts: list[Alert] = []
        pr = scc = None

        # Analyses les plus lourdes un passage sur deux (~3 min) : elles ne doivent pas
        # affamer le chemin rapide sur une machine partagée.
        heavy = self.slow_batches % 2 == 0
        if heavy and not hv.isEmpty():
            # PageRank + SCC sur le graphe des gros flux
            t = time.time()
            pr = D.pagerank(hv).localCheckpoint(eager=True)
            self.pagerank_pct = {r.id: r.pagerank_pct for r in
                                 pr.filter(F.col("pagerank_pct") >= 0.9).select("id", "pagerank_pct").collect()}
            timings["pagerank_ms"] = ms(t)
            t = time.time()
            scc = D.strongly_connected(hv).localCheckpoint(eager=True)
            timings["scc_ms"] = ms(t)

            # Cycles longs (4-5 sauts) à l'intérieur des SCC
            t = time.time()
            for n, found in D.long_cycles_in_sccs(hv, scc, (4, 5), S.window_seconds).items():
                for r in found:
                    edges = self.normalize(D.rows_to_cycle_edges(r, n))
                    sc = score_cycle(edges, self.pagerank_pct)
                    if sc.level is None:
                        continue
                    roles = {e["src"]: "RELAIS" for e in edges}
                    roles[edges[0]["src"]] = "ORIGINE"
                    alerts.append(make_alert("CYCLE_LONG", [e["transaction_id"] for e in edges], sc,
                                             [e["src"] for e in edges], edges, explain_cycle(edges, sc),
                                             detected, roles, cycle_length=n))
            timings["long_cycles_ms"] = ms(t)

        # Comptes mules (fan-in puis retrait)
        t = time.time()
        candidates = D.find_mules(win, S.mule_min_senders, S.mule_min_inflow_mad,
                                  S.mule_flow_through_min).collect()
        # Pic de vélocité : rafale >= 3x le rythme habituel du compte (journal des dépôts en mémoire)
        mules, baselines = [], {}
        for m in candidates:
            spike, base = velocity_spike(self.deposits.get(m.account), utc(m.big_out_time), m.n_deposits,
                                         self.deposits.start or utc(m.big_out_time))
            if spike:
                mules.append(m)
                baselines[m.account] = base
        if mules:
            accs = [m.account for m in mules]
            sub = win.filter(F.col("src").isin(accs) | F.col("dst").isin(accs)).collect()
            for m in mules:
                outs = self.normalize([r.asDict() for r in sub if r.src == m.account])
                big = max(outs, key=lambda e: e["montant"])
                # dépôts de la rafale précédant le retrait (comme dans find_mules)
                inc = self.normalize([r.asDict() for r in sub if r.dst == m.account
                                      and big["event_time"] - timedelta(minutes=6) <= utc(r.event_time)
                                      < big["event_time"]])
                delay = (big["event_time"] - max(e["event_time"] for e in inc)).total_seconds()
                burst = (utc(m.last_in) - utc(m.first_in)).total_seconds()
                sc = score_mule(m.n_senders, m.inflow, big["montant"], list(m.incoming), delay, burst,
                                baseline_per_burst=baselines.get(m.account))
                roles = {e["src"]: "SOURCE" for e in inc}
                roles[m.account] = "MULE"
                roles[big["dst"]] = "DESTINATION"
                alerts.append(make_alert("MULE", [m.account, day], sc,
                                         [m.account] + [e["src"] for e in inc] + [big["dst"]], inc + [big],
                                         explain_mule(m.account, sc), detected, roles))
        timings["mules_ms"] = ms(t)

        # Scatter-gather (motif GraphFrames à 2 sauts)
        t = time.time()
        for r in D.find_scatter_gather(win, S.scatter_min_intermediaries, S.window_seconds).collect():
            outs = {e.transaction_id: e.asDict() for e in r.out_edges}
            ins = {e.transaction_id: e.asDict() for e in r.in_edges}
            sc = score_scatter_gather(r.k, [e["montant"] for e in outs.values()],
                                      [e["montant"] for e in ins.values()])
            mids = sorted({e["dst"] for e in outs.values()})
            roles = {m: "RELAIS" for m in mids}
            roles.update({r.source: "SOURCE", r.sink: "DESTINATION"})
            alerts.append(make_alert("SCATTER_GATHER", [r.source, r.sink, day], sc, [r.source, *mids, r.sink],
                                     self.normalize(list(outs.values()) + list(ins.values())),
                                     explain_scatter_gather(r.source, r.sink, sc), detected, roles))
        timings["scatter_ms"] = ms(t)

        # Pivots : comptes présents dans plusieurs cycles récents (+ PageRank)
        t = time.time()
        recent = (self.delta("fraud_alerts").filter(F.col("dt").isin(self.recent_dt()))
                  .filter(F.col("alert_type").isin("CYCLE", "CYCLE_LONG", "MULE", "SCATTER_GATHER"))
                  .filter(F.col("window_end") >= F.lit(mx - timedelta(seconds=S.window_seconds)))
                  .filter(F.col("risk_level") != "LOW")
                  .select("alert_id", "alert_type", "accounts", "transaction_ids").collect())
        recent_alerts = [(r.alert_id, r.alert_type, list(r.accounts)) for r in recent]
        recent_alerts += [(a.alert_id, a.alert_type, a.accounts) for a in alerts if a.risk_level != "LOW"]
        recent_tx = {tx for r in recent for tx in (r.transaction_ids or [])}
        recent_tx |= {tx for a in alerts for tx in a.transaction_ids}
        cycles_by_acc: dict[str, set[str]] = defaultdict(set)
        for aid, typ, accs in recent_alerts:
            if typ in ("CYCLE", "CYCLE_LONG"):
                for acc in accs:
                    cycles_by_acc[acc].add(aid)
        pivots = [acc for acc, ids in cycles_by_acc.items() if len(ids) >= S.pivot_min_cycles]
        suspicious = sorted({acc for _, _, accs in recent_alerts for acc in accs})
        sus_rows = []
        if suspicious:
            sus_rows = [r.asDict() for r in win.filter(F.col("src").isin(suspicious) | F.col("dst").isin(suspicious))
                        .filter(F.col("transaction_id").isin(list(recent_tx))).collect()]
        for acc in pivots:
            edges = self.normalize([dict(e) for e in sus_rows if acc in (e["src"], e["dst"])])
            sc = score_pivot(len(cycles_by_acc[acc]), self.pagerank_pct.get(acc))
            others = sorted(({e["src"] for e in edges} | {e["dst"] for e in edges}) - {acc})
            roles = {o: "MEMBRE" for o in others}
            roles[acc] = "PIVOT"
            alerts.append(make_alert("PIVOT", [acc, day], sc, [acc, *others], edges,
                                     explain_pivot(acc, sc), detected, roles))
        timings["pivot_ms"] = ms(t)

        # Connected Components (GraphFrames) sur le graphe suspect -> réseaux reliant plusieurs alertes
        t = time.time()
        clusters: dict[str, list[str]] = {}
        if len(sus_rows) >= 3:
            sus_edges = self.spark.createDataFrame(  # via Arrow : pas de worker Python
                pd.DataFrame({"src": [e["src"] for e in sus_rows], "dst": [e["dst"] for e in sus_rows]}),
                "src STRING, dst STRING")
            for r in D.connected_components(sus_edges).collect():
                clusters.setdefault(r.cluster_id, []).append(r.id)
            for members in clusters.values():
                mset = set(members)
                linked = {aid for aid, _, accs in recent_alerts if mset & set(accs)}
                if len(linked) >= 2 and len(members) >= 4:
                    edges = self.normalize([dict(e) for e in sus_rows if e["src"] in mset and e["dst"] in mset])
                    sc = score_cluster(len(members), len(linked), sum(e["montant"] for e in edges))
                    hub = max(members, key=lambda a: (len(cycles_by_acc.get(a, ())), self.pagerank_pct.get(a, 0)))
                    roles = {m: "MEMBRE" for m in members}
                    roles[hub] = "PIVOT"
                    alerts.append(make_alert("CLUSTER", sorted(members), sc, sorted(members), edges,
                                             explain_cluster(members, sc), detected, roles))
        timings["clusters_ms"] = ms(t)

        # Émission des nouvelles alertes uniquement (idempotence)
        new_alerts = [a for a in {a.alert_id: a for a in alerts}.values() if a.alert_id not in self.emitted]
        self.publish(new_alerts)
        self.write_alerts(new_alerts, app, "slow-path", batch_id)
        self.emitted.update(a.alert_id for a in new_alerts)
        for a in new_alerts:
            if level_at_least(a.risk_level, "MEDIUM"):
                log(f"[ALERTE {a.risk_level:<8}] {a.alert_type} score={a.risk_score} — {a.explanation[:140]}")

        # Instantané des métriques de compte (top PageRank + comptes suspects)
        t = time.time()
        n_alerts: dict[str, int] = defaultdict(int)
        for _, _, accs in recent_alerts + [(a.alert_id, a.alert_type, a.accounts) for a in new_alerts]:
            for acc in set(accs):
                n_alerts[acc] += 1
        if pr is not None:
            self.write_account_metrics(win, pr, scc, clusters, n_alerts, app, batch_id)
        timings["metrics_ms"] = ms(t)

        # Compactage périodique du bronze (petits fichiers des micro-batches)
        self.slow_batches += 1
        if self.slow_batches % 10 == 0:
            t = time.time()
            from delta.tables import DeltaTable
            DeltaTable.forPath(self.spark, S.table("transactions")).optimize().where(
                f"dt = '{day}'").executeCompaction()
            timings["optimize_ms"] = ms(t)

        for df in (win, hv, pr, scc):
            D.release(df)
        by_type: dict[str, int] = defaultdict(int)
        for a in new_alerts:
            by_type[a.alert_type] += 1
        self.slow_log.write({"ts": now_utc().isoformat(), "batch_id": batch_id, "window_edges": n_edges,
                             "new_alerts": dict(by_type), "total_ms": ms(t0), **timings})
        log(f"[slow-path] batch {batch_id}: {n_edges} arêtes, {len(new_alerts)} nouvelles alertes "
            f"{dict(by_type)} en {time.time() - t0:.1f} s")

    def write_account_metrics(self, win, pr, scc, clusters, n_alerts, app, batch_id) -> None:
        top = {r.id: (r.pagerank, r.pagerank_pct) for r in
               pr.orderBy(F.desc("pagerank")).limit(300).collect()} if pr is not None else {}
        keep = set(top) | set(n_alerts)
        if not keep:
            return
        extra = [a for a in n_alerts if a not in top]
        if pr is not None and extra:
            top.update({r.id: (r.pagerank, r.pagerank_pct)
                        for r in pr.filter(F.col("id").isin(extra)).collect()})
        sccs = {r.id: (r.scc_id, r.scc_size) for r in scc.filter(F.col("id").isin(list(keep))).collect()} \
            if scc is not None else {}
        sub = win.filter(F.col("src").isin(list(keep)) | F.col("dst").isin(list(keep)))
        outs = {r.a: r for r in sub.groupBy(F.col("src").alias("a")).agg(
            F.countDistinct("dst").alias("deg"), F.sum("montant").alias("amt")).collect()}
        ins = {r.a: r for r in sub.groupBy(F.col("dst").alias("a")).agg(
            F.countDistinct("src").alias("deg"), F.sum("montant").alias("amt")).collect()}
        cluster_of = {m: cid for cid, members in clusters.items() for m in members}
        snap = now_utc()
        sid = f"SNAP-{snap.strftime('%Y%m%d%H%M%S')}-{batch_id}"
        rows = []
        for acc in keep:
            pagerank, pct = top.get(acc, (None, None))
            inflow = ins[acc].amt if acc in ins else 0.0
            outflow = outs[acc].amt if acc in outs else 0.0
            scc_id, scc_size = sccs.get(acc, (None, None))
            rows.append({"snapshot_id": sid, "snapshot_time": snap, "account_id": acc, "pagerank": pagerank,
                         "pagerank_pct": pct, "in_degree": ins[acc].deg if acc in ins else 0,
                         "out_degree": outs[acc].deg if acc in outs else 0, "inflow": inflow, "outflow": outflow,
                         "flow_through": outflow / inflow if inflow else None, "scc_id": scc_id,
                         "scc_size": scc_size, "cluster_id": cluster_of.get(acc),
                         "n_alerts": n_alerts.get(acc, 0), "dt": snap.strftime("%Y-%m-%d")})
        self._write("account_metrics", rows, app, batch_id)

    # -- trafic en fenêtre glissante (API native window + watermark) --------------
    def traffic_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        from delta.tables import DeltaTable
        rows = (batch_df.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                                "tx_count", "total_amount", "high_value_count", "active_accounts")
                .withColumn("updated_at", F.current_timestamp())
                .withColumn("dt", F.date_format("window_start", "yyyy-MM-dd")))
        (DeltaTable.forPath(self.spark, S.table("traffic_windows")).alias("t")
         .merge(rows.alias("s"), "t.dt = s.dt AND t.window_start = s.window_start")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

    # -- démarrage ----------------------------------------------------------------
    def start(self) -> None:
        from confluent_kafka import Producer

        spark, sc = self.spark, self.spark.sparkContext
        self.kafka = Producer({"bootstrap.servers": S.kafka_bootstrap, "client.id": "spark-fraud-alerts",
                               "acks": "all", "linger.ms": 0, "enable.idempotence": True})
        self.app_ids = {"ingest": self.app_id("ingest"), "slow-path": self.app_id("slow-path")}
        spark.streams.addListener(MetricsListener(self.progress_log))
        self.recover()
        wm = f"{S.watermark_minutes} minutes"

        sc.setLocalProperty("spark.scheduler.pool", "fast")
        (parse(read_kafka(spark, max_offsets=5_000)).writeStream.queryName("ingest").foreachBatch(self.ingest)
         .option("checkpointLocation", self.ckpt("ingest"))
         .trigger(processingTime=os.environ.get("INGEST_TRIGGER", "1 second")).start())

        sc.setLocalProperty("spark.scheduler.pool", "ops")
        traffic = (parse(read_kafka(spark)).filter(F.col("error").isNull())
                   .withWatermark("event_time", wm)
                   .groupBy(F.window("event_time", f"{S.window_minutes} minutes", f"{S.slide_minutes} minute"))
                   .agg(F.count("*").alias("tx_count"), F.sum("montant").alias("total_amount"),
                        F.sum(F.when(F.col("montant") >= S.high_value_mad, 1).otherwise(0)).cast("long")
                        .alias("high_value_count"),
                        F.approx_count_distinct("source_account").alias("active_accounts")))
        (traffic.writeStream.queryName("traffic-windows").outputMode("update").foreachBatch(self.traffic_batch)
         .option("checkpointLocation", self.ckpt("traffic-windows")).trigger(processingTime="60 seconds").start())

        sc.setLocalProperty("spark.scheduler.pool", "slow")
        (spark.readStream.format("delta").option("startingVersion", "latest").load(S.table("transactions"))
         .writeStream.queryName("slow-path").foreachBatch(self.slow_path)
         .option("checkpointLocation", self.ckpt("slow-path"))
         .trigger(processingTime=f"{S.slow_path_seconds} seconds").start())
        sc.setLocalProperty("spark.scheduler.pool", None)

        log(f"[start] 3 requêtes actives (ingest, traffic-windows, slow-path) — Kafka={S.kafka_bootstrap}, "
            f"fenêtre={S.window_minutes} min, watermark={wm}, Spark UI http://localhost:4040")
        spark.streams.awaitAnyTermination()


def main() -> int:
    spark = build_spark("spark-graph-fraud-streaming")
    ensure_schema(spark)
    pipeline = FraudGraphPipeline(spark)

    def shutdown(*_):
        log("[stop] arrêt demandé, fermeture propre des requêtes…")
        for q in spark.streams.active:
            q.stop()

    signal.signal(signal.SIGTERM, shutdown)
    try:
        pipeline.start()
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        if pipeline.kafka is not None:
            pipeline.kafka.flush(5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
