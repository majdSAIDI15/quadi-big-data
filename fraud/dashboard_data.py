"""Lecture du lakehouse pour le dashboard, sans JVM (delta-rs / pyarrow)."""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from deltalake import DeltaTable

from .config import settings as S

DISPLAY_TZ = os.environ.get("DISPLAY_TZ", "UTC")


def utcnow() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def recent_dts(days: int = 1) -> list[str]:
    now = datetime.now(timezone.utc)
    return [(now - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(days, -1, -1)]


def table_exists(key: str) -> bool:
    return os.path.isdir(os.path.join(S.table(key), "_delta_log"))


def delta(key: str, version: int | None = None) -> DeltaTable | None:
    if not table_exists(key):
        return None
    return DeltaTable(S.table(key), version=version) if version is not None else DeltaTable(S.table(key))


def read(key: str, expr=None, columns: list[str] | None = None, version: int | None = None,
         days: int = 1) -> pd.DataFrame:
    """Lit une table Delta en élaguant les partitions `dt` (et filtre pyarrow optionnel)."""
    t = delta(key, version)
    if t is None:
        return pd.DataFrame(columns=columns or [])
    try:
        flt = ds.field("dt").isin(recent_dts(days))
        if expr is not None:
            flt = flt & expr
        return t.to_pyarrow_dataset().to_table(filter=flt, columns=columns).to_pandas()
    except Exception:
        return pd.DataFrame(columns=columns or [])


def to_local(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s, utc=True)
    return s.dt.tz_convert(DISPLAY_TZ)


# --------------------------------------------------------------------------- alertes
def alerts(since: pd.Timestamp) -> pd.DataFrame:
    df = read("fraud_alerts", days=1 if since > utcnow() - pd.Timedelta(hours=20) else 2)
    if df.empty:
        return df
    df["detected_at"] = pd.to_datetime(df["detected_at"], utc=True)
    df = df[df["detected_at"] >= since].sort_values("detected_at", ascending=False)
    df["features"] = df["features"].apply(lambda x: json.loads(x) if isinstance(x, str) and x else {})
    df["accounts"] = df["accounts"].apply(list)
    df["transaction_ids"] = df["transaction_ids"].apply(lambda x: list(x) if x is not None else [])
    return df.reset_index(drop=True)


def subgraphs(alert_ids: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not alert_ids:
        return pd.DataFrame(), pd.DataFrame()
    f = ds.field("alert_id").isin(alert_ids)
    return read("subgraph_vertices", f, days=2), read("subgraph_edges", f, days=2)


# --------------------------------------------------------------------------- graphe & trafic
def account_edges(account: str, since: pd.Timestamp, limit: int = 400) -> pd.DataFrame:
    f = (ds.field("source_account") == account) | (ds.field("target_account") == account)
    cols = ["transaction_id", "source_account", "target_account", "montant", "event_time", "type_operation"]
    df = read("transactions", f, cols)
    if df.empty:
        return df
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    return df[df["event_time"] >= since].sort_values("montant", ascending=False).head(limit)


def traffic(since: pd.Timestamp) -> pd.DataFrame:
    df = read("traffic_windows")
    if df.empty:
        return df
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    # Fenêtres glissantes complètes uniquement (celles qui finissent dans le futur sont partielles)
    df = df[(df["window_end"] >= since) & (df["window_end"] <= utcnow())]
    return df.sort_values("window_end")


def latest_metrics() -> pd.DataFrame:
    df = read("account_metrics")
    if df.empty:
        return df
    return df[df["snapshot_time"] == df["snapshot_time"].max()]


def quarantine(since: pd.Timestamp) -> pd.DataFrame:
    df = read("quarantine", columns=["error", "kafka_timestamp"])
    if df.empty:
        return df
    df["kafka_timestamp"] = pd.to_datetime(df["kafka_timestamp"], utc=True)
    return df[df["kafka_timestamp"] >= since]


# --------------------------------------------------------------------------- métriques & audit
def jsonl_tail(name: str, max_lines: int = 3000) -> pd.DataFrame:
    path = os.path.join(S.metrics_dir, name)
    if not os.path.exists(path):
        return pd.DataFrame()
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        start = max(0, f.tell() - max_lines * 600)
        f.seek(start)
        lines = f.read().decode("utf-8", errors="ignore").splitlines()
        if start > 0:
            lines = lines[1:]  # première ligne tronquée
    rows = []
    for line in lines[-max_lines:]:
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    df = pd.DataFrame(rows)
    if "ts" in df:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")
    return df


def history(key: str, limit: int = 30) -> pd.DataFrame:
    t = delta(key)
    if t is None:
        return pd.DataFrame()
    h = pd.DataFrame(t.history(limit))
    if "timestamp" in h:
        h["timestamp"] = pd.to_datetime(h["timestamp"], unit="ms", utc=True)
    return h


def partition_layout(key: str) -> pd.DataFrame:
    t = delta(key)
    if t is None:
        return pd.DataFrame()
    files = pa.record_batch(t.get_add_actions(flatten=True)).to_pandas()
    part_cols = [c for c in files.columns if c.startswith("partition.")]
    if not part_cols:
        return pd.DataFrame()
    g = files.groupby(part_cols).agg(fichiers=("path", "count"), lignes=("num_records", "sum"),
                                     octets=("size_bytes", "sum")).reset_index()
    g.columns = [c.replace("partition.", "") for c in g.columns]
    return g
