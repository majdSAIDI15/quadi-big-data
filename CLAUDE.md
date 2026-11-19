# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Exam project (Mini-Projet 17, brief in the PDF at the repo root, in French): streaming graph analytics to detect carousel fraud and money-laundering loops. Kafka → Spark Structured Streaming + GraphFrames → Delta Lake → Streamlit/PyVis dashboard. User-facing text, comments, docs and alert explanations are in French; keep it that way.

The four deliverable filenames at the repo root are imposed by the brief — do not rename them: `financial_graph_producer.py`, `spark_graph_fraud_streaming.py`, `fraud_graph_delta_schema.py`, `graph_network_dashboard.py`. Shared code lives in `fraud/`.

## Commands

Everything runs in Docker (host has Java 23, which Spark 3.5 does not support, and no winutils). The image bakes Java 17 and pre-resolves the Kafka/Delta/GraphFrames jars into `/opt/ivy/jars` (offline-capable).

```bash
docker compose up -d --build                 # kafka, kafka-init (topics), spark-streaming, producer, dashboard
docker compose --profile tools up -d kafka-ui
docker compose down -v                       # reset ALL data (lakehouse volume + checkpoints)
docker build -t fraud-graph-app:latest .     # needed after code changes (code is COPY'd, not mounted)

# tests / lint (inside the image; Spark tests need the JVM)
docker compose run --rm --no-deps -e CHECKPOINT_DIR=/tmp/ck spark-streaming sh -c "ruff check . && pytest -q"
docker compose run --rm --no-deps -e CHECKPOINT_DIR=/tmp/ck spark-streaming pytest -q tests/test_graph_detection.py -k long_cycle
pytest -q -m "not spark"                     # pure-Python tests also run on the host (needs pytest only)

# evaluation / benchmark (mount docs/ to get the reports back on the host)
docker compose run --rm --no-deps -v "$(pwd -W)/docs:/app/docs" spark-streaming python scripts/evaluate.py [--ablation]
docker compose stop producer && docker compose run --rm --no-deps -v "$(pwd -W)/docs:/app/docs" producer python scripts/benchmark.py
docker compose exec dashboard python scripts/check_integrity.py   # duplicates in bronze/gold after a crash
```

`scripts/demo.ps1` wraps these for Windows PowerShell (`start`, `inject`, `crash`, `evaluate`, `ablation`, `benchmark`, `tests`, `reset`). In Git Bash, prefix `docker ... -v` commands that use container paths with `MSYS_NO_PATHCONV=1`.

Runtime metrics are JSONL files in the volume: `/data/metrics/{fast_path,slow_path,pipeline_metrics}.jsonl` — the first place to look when diagnosing latency.

## Architecture (the parts that need several files to understand)

One Spark app, three streaming queries (`spark_graph_fraud_streaming.py`, `start()`):

- **`ingest`** (Kafka, trigger 1 s, `foreachBatch`): parses, quarantines invalid rows, applies the 2-min watermark and dedup **in Python on the driver**, adds fresh edges to `fraud/graph_index.py::SlidingGraphIndex` (in-memory adjacency index, 10 + 2 min retention), prunes triangle candidates in O(batch) via `triangle_candidate_ids`, then runs the GraphFrames motif only on that small subgraph. Order of writes is deliberate: Kafka alert → gold → bronze (a crash between steps replays the batch, re-detects, and idempotent writes absorb it).
- **`traffic-windows`** (Kafka, 60 s): the native `window(10 min, 1 min)` + `withWatermark(2 min)` aggregation → MERGE into `silver/traffic_windows`.
- **`slow-path`** (Delta stream on bronze as a timer, 90 s): reads the 10-min window from bronze; PageRank and SCC on the high-value graph (≥ 20 000 MAD), long cycles inside SCCs, mules, scatter-gather, pivots (accounts in ≥ 2 recent cycle alerts), Connected Components on the already-alerted subgraph, account-metrics snapshot. Deliberately coalesced to 1–2 partitions so it doesn't starve the fast path.

Performance lessons baked into the code (don't undo without measuring):
- Small DataFrames built on the driver go through **Arrow + `localCheckpoint`** (`detection.frame_from_rows`, `_write`). Plain `createDataFrame(list)` re-runs Python workers on every action (~1 s each); a `localCheckpoint` keeps stats so GraphFrames joins become broadcasts. `spark.sql.defaultSizeInBytes=8MB` is set for the same reason.
- Never pass thousands of literals to `isin()` (one py4j round trip per value).
- Any DataFrame reused by a GraphFrames motif must be materialized first, or the lineage is recomputed once per edge/vertex alias (dozens of jobs).
- Iterative loops (`trim_acyclic`) must `localCheckpoint` each round, or the plan doubles every iteration (OOM).
- Do not re-enable the RocksDB state store: it crashed the JVM ("pthread lock: Invalid argument").
- `DataFrame.unpersist()` does NOT free a `localCheckpoint`; its blocks stay until Spark's periodic GC. In this long-running job that leaked ~2 GB in 30 min and stalled Delta writes. Always call `detection.release(df)` on materialized DataFrames once used (fast-path candidates, slow-path window/hv/pr/scc, trim rounds); `spark.cleaner.periodicGC.interval=2min` is the safety net. Watch `docker stats fraud-spark` and the Spark UI storage tab (cached RDD count should stay flat).
- The slow path runs PageRank/SCC/long cycles/account metrics only every other pass (`heavy = slow_batches % 2 == 0`); mules use the in-memory `DepositLog` (fed by the fast path, rebuilt from 40 min of bronze on recovery) instead of a Spark join over history.
- Measured on this laptop (8 vCPU, ~6 GB for Docker): carousel alert latency ~2.5–5 s at 50 tx/s; slow path ~40–60 s per run.

Correctness invariants:
- `alert_id` = hash of alert type + sorted keys (transaction ids for cycles; account + day for MULE/PIVOT; source/sink/day for scatter-gather). Every Delta write uses `txnAppId`/`txnVersion` (app id stored under `checkpoints/_txn/`, regenerated when a checkpoint is deleted). The slow path additionally keeps an in-memory `emitted` set rebuilt from gold at startup.
- Cycles must be temporally ordered (e1 < e2 < e3, span ≤ window); this is also what makes each cycle appear in exactly one rotation.
- Mule rule (`find_mules`) is the result of several measured iterations: only deposits in the 6 min before the largest outgoing transfer count, and they must be a velocity spike (≥ 3× the account's usual rate over the previous 20 min, read from bronze). Without the spike criterion, steady-income merchants become false positives; without the "before the cash-out" restriction, later legitimate deposits hide real mules. Needs ~12 min of history (cold start), so evaluation runs use `FIRST_FRAUD_AFTER=720`. Validate rule changes offline against `ground-truth` before restarting the stack.
- `fraud_graph_delta_schema.py::DEFS` is the single source of truth for table schemas; `fraud/config.py::TABLES` must list the same keys (asserted). Gold tables are `appendOnly` and partitioned by `risk_level, dt`; bronze has CHECK constraints (filter rows before writing or the batch fails).
- Scoring (`fraud/scoring.py`) and scenario generation (`fraud/scenarios.py`) are pure Python and shared by producer, Spark job, dashboard and tests. Levels: LOW ≥ 20, MEDIUM ≥ 40, HIGH ≥ 70, CRITICAL ≥ 90; only ≥ MEDIUM go to Kafka.

The dashboard reads Delta with delta-rs (`fraud/dashboard_data.py`, no JVM); delta-rs 1.x returns arro3 objects — wrap with `pa.record_batch(...)` before `.to_pandas()`. Streamlit fragments cannot write into containers created outside them. Chart colors follow `fraud/viz.py` (status palette = risk level, always paired with its label; fixed categorical order for alert types).
