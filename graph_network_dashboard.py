"""Livrable 4 — Dashboard Streamlit + PyVis de navigation dans le graphe de fraude.

Onglets : vue d'ensemble (temps réel), alertes & fiches d'investigation, explorateur
de réseau, santé du pipeline, audit Delta (historique, partitions, time travel).
La barre latérale permet d'injecter une fraude en direct pendant la démo.

Lancement : streamlit run graph_network_dashboard.py
"""
from __future__ import annotations

import random
import time

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from fraud import dashboard_data as data
from fraud import viz
from fraud.config import settings
from fraud.scenarios import SCENARIO_TYPES, AccountPopulation, build_scenario

st.set_page_config(page_title="Graph Analytics — Blanchiment", page_icon="🕸️", layout="wide")

SCENARIO_LABEL = {"CARROUSEL": "Carrousel 3 comptes", "CYCLE_LONG": "Cycle long (4-5 comptes)",
                  "FAN_IN_MULE": "Compte mule (smurfing)", "SCATTER_GATHER": "Scatter-gather",
                  "PIVOT_HUB": "Pivot au centre de 3 carrousels"}
PERIODS = {"15 dernières minutes": 15, "Dernière heure": 60, "6 dernières heures": 360, "24 dernières heures": 1440}

st.markdown("""
<style>
  .block-container {padding-top: 1.6rem; padding-bottom: 2rem;}
  div[data-testid="stMetricValue"] {font-variant-numeric: tabular-nums;}
  .badge {display:inline-block; padding:1px 8px; border-radius:6px; font-size:0.78rem; font-weight:600;}
  .alert-card {border:1px solid rgba(255,255,255,0.10); border-radius:10px; padding:8px 12px; margin-bottom:8px;
               background:#1a1a19;}
  .alert-card .meta {color:#c3c2b7; font-size:0.82rem;}
  .alert-card .why {color:#f0efec; font-size:0.86rem; margin-top:2px;}
</style>""", unsafe_allow_html=True)


def badge(level: str) -> str:
    color = viz.LEVEL_COLORS.get(level, viz.MUTED)
    ink = "#0b0b0b" if level == "MEDIUM" else "#ffffff"
    return f"<span class='badge' style='background:{color};color:{ink}'>{viz.LEVEL_LABEL.get(level, level)}</span>"


def local(ts) -> str:
    return pd.Timestamp(ts).tz_convert(data.DISPLAY_TZ).strftime("%H:%M:%S")


def level_filter(df: pd.DataFrame, minimum: str) -> pd.DataFrame:
    if df.empty:
        return df
    return df[df["risk_level"].map(viz.LEVEL_RANK) >= viz.LEVEL_RANK[minimum]]


@st.cache_resource
def kafka_producer():
    from fraud.kafka_io import make_producer
    return make_producer(client_id="dashboard-injector")


@st.cache_resource
def population():
    pop = AccountPopulation(10_000, seed=42)
    pop.rng = random.Random()  # comptes « écrans » différents de ceux du producteur
    return pop


@st.cache_data(ttl=3, show_spinner=False)
def load_alerts(minutes: int) -> pd.DataFrame:
    return data.alerts(data.utcnow() - pd.Timedelta(minutes=minutes))


@st.cache_data(ttl=3, show_spinner=False)
def load_subgraphs(ids: tuple[str, ...]):
    return data.subgraphs(list(ids))


@st.cache_data(ttl=5, show_spinner=False)
def load_metrics(name: str) -> pd.DataFrame:
    return data.jsonl_tail(name)


# =========================================================================== barre latérale
with st.sidebar:
    st.markdown("## 🕸️ Network Intelligence")
    st.caption("Détection de blanchiment et de fraude carrousel — Spark Structured Streaming · "
               "GraphFrames · Delta Lake")
    period_label = st.selectbox("Période", list(PERIODS), index=1)
    minutes = PERIODS[period_label]
    min_level = st.select_slider("Niveau minimal affiché", viz.LEVEL_ORDER, value="MEDIUM",
                                 format_func=lambda x: viz.LEVEL_LABEL[x])
    live = st.toggle("Rafraîchissement temps réel", value=True)

    st.divider()
    st.markdown("### 💉 Injecter une fraude")
    kind = st.selectbox("Scénario", SCENARIO_TYPES, format_func=lambda k: SCENARIO_LABEL[k])
    if st.button("Injecter maintenant", type="primary", use_container_width=True):
        try:
            from fraud.kafka_io import inject_in_background
            sc = build_scenario(kind, population(), tempo=0.05)
            inject_in_background(kafka_producer(), sc)
            st.session_state.setdefault("injected", []).insert(0, {
                "id": sc.scenario_id, "type": kind, "desc": sc.description, "tx": set(sc.transaction_ids),
                "at": data.utcnow(), "duration": sc.duration_seconds})
            st.toast(f"Fraude injectée : {sc.description}", icon="💉")
        except Exception as exc:  # Kafka indisponible
            st.error(f"Injection impossible : {exc}")
    st.caption("Les étapes sont rejouées en accéléré (quelques secondes). Les carrousels sont détectés par "
               "le chemin rapide ; mules, scatter-gather, cycles longs et pivots par le chemin lent (≤ 90 s).")


def injected_status() -> None:
    """Suivi des fraudes injectées : détectée ou non, et en combien de secondes."""
    items = st.session_state.get("injected", [])[:6]
    if not items:
        return
    alerts_df = load_alerts(60)
    st.markdown("**Fraudes injectées dans cette session**")
    for it in items:
        hit = alerts_df[alerts_df["transaction_ids"].apply(lambda t, tx=it["tx"]: bool(tx & set(t)))] \
            if not alerts_df.empty else alerts_df
        if not hit.empty:
            first = hit.sort_values("detected_at").iloc[0]
            secs = (first["detected_at"] - it["at"]).total_seconds()
            st.markdown(f"✅ {SCENARIO_LABEL[it['type']]} — **détectée** "
                        f"({viz.TYPE_LABEL.get(first['alert_type'])}, {viz.LEVEL_LABEL[first['risk_level']]}) "
                        f"{secs:.0f} s après injection")
        else:
            waited = (data.utcnow() - it["at"]).total_seconds()
            st.markdown(f"⏳ {SCENARIO_LABEL[it['type']]} — en cours ({waited:.0f} s)")


with st.sidebar:
    st.fragment(run_every=3 if live else None)(injected_status)()
    st.divider()
    st.caption(f"Fenêtre glissante {settings.window_minutes} min · watermark {settings.watermark_minutes} min · "
               f"heures en {data.DISPLAY_TZ}")


# =========================================================================== en-tête
st.markdown("# Graph Analytics & Blanchiment d'argent")
tab_live, tab_alerts, tab_net, tab_health, tab_audit = st.tabs(
    ["🛰️ Vue d'ensemble", "🚨 Alertes & investigation", "🕸️ Explorateur de réseau",
     "⚙️ Santé du pipeline", "🗄️ Audit Delta Lake"])


# =========================================================================== vue d'ensemble
def overview() -> None:
    all_alerts = load_alerts(max(minutes, 15))
    alerts = level_filter(all_alerts[all_alerts["detected_at"] >= data.utcnow() - pd.Timedelta(minutes=minutes)]
                          if not all_alerts.empty else all_alerts, min_level)
    fast = load_metrics("fast_path.jsonl")
    traffic = data.traffic(data.utcnow() - pd.Timedelta(minutes=minutes))

    # Notifications des nouvelles alertes graves
    seen = st.session_state.setdefault("seen_alerts", set())
    fresh = alerts[~alerts["alert_id"].isin(seen)] if not alerts.empty else alerts
    if seen and not fresh.empty:
        for r in fresh[fresh["risk_level"].isin(["HIGH", "CRITICAL"])].head(3).itertuples():
            st.toast(f"{viz.LEVEL_LABEL[r.risk_level]} · {viz.TYPE_LABEL.get(r.alert_type)} · "
                     f"{' → '.join(r.accounts[:4])}", icon="🚨")
    seen.update(alerts["alert_id"].tolist() if not alerts.empty else [])

    c = st.columns(5)
    last_win = traffic.iloc[-1] if not traffic.empty else None
    c[0].metric("Transactions (fenêtre 10 min)", f"{int(last_win.tx_count):,}".replace(",", " ")
                if last_win is not None else "—")
    if not fast.empty:
        recent = fast[fast["ts"] >= data.utcnow() - pd.Timedelta(seconds=60)]
        rate = recent["rows"].sum() / 60 if not recent.empty else 0
        c[1].metric("Débit traité", f"{rate:,.0f} tx/s")
    else:
        c[1].metric("Débit traité", "—")
    c[2].metric(f"Alertes ≥ {viz.LEVEL_LABEL[min_level].lower()}", len(alerts))
    c[3].metric("Critiques", int((alerts["risk_level"] == "CRITICAL").sum()) if not alerts.empty else 0)
    lat = alerts[(alerts["detector"] == "fast-path")]["latency_ms"].dropna() if not alerts.empty else pd.Series()
    c[4].metric("Latence carrousel p50 / p95",
                f"{lat.median() / 1000:.1f} s / {lat.quantile(0.95) / 1000:.1f} s" if len(lat) else "—")

    left, right = st.columns([3, 2])
    with left:
        st.markdown("#### Réseau suspect en direct")
        top = alerts.head(25)
        if top.empty:
            st.info("Aucune alerte sur la période. Injectez une fraude depuis la barre latérale.")
        else:
            v, e = load_subgraphs(tuple(top["alert_id"]))
            if not e.empty:
                components.html(viz.network_html(e, v, height=500, physics=True), height=510)
            st.caption("Couleur = niveau de risque · forme = rôle (★ origine, ◆ pivot, ▲ mule, ■ destination) · "
                       "épaisseur = montant")
    with right:
        st.markdown("#### Dernières alertes")
        if alerts.empty:
            st.caption("—")
        for r in alerts.head(7).itertuples():
            st.markdown(
                f"<div class='alert-card'>{badge(r.risk_level)} &nbsp;<b>{viz.TYPE_LABEL.get(r.alert_type)}</b>"
                f" · score {r.risk_score}<div class='meta'>{local(r.detected_at)} · "
                f"{viz.fmt_mad(r.total_amount)} · {len(r.accounts)} comptes"
                f"{f' · détectée en {r.latency_ms / 1000:.1f} s' if pd.notna(r.latency_ms) else ''}</div>"
                f"<div class='why'>{r.explanation[:190]}{'…' if len(r.explanation) > 190 else ''}</div></div>",
                unsafe_allow_html=True)

    g1, g2 = st.columns(2)
    with g1:
        if not alerts.empty:
            per = alerts.assign(minute=alerts["detected_at"].dt.floor("min").dt.tz_convert(data.DISPLAY_TZ))
            per = per.groupby(["minute", "risk_level"]).size().reset_index(name="n")
            st.plotly_chart(viz.stacked_bars(per, "minute", "risk_level", viz.LEVEL_ORDER, viz.LEVEL_COLORS,
                                             viz.LEVEL_LABEL, "Alertes par minute et niveau de risque",
                                             "alertes"), use_container_width=True, theme=None)
    with g2:
        if not traffic.empty:
            t = traffic.assign(fin=traffic["window_end"].dt.tz_convert(data.DISPLAY_TZ))
            st.plotly_chart(viz.line(t, "fin", "tx_count", "Transactions par fenêtre glissante 10 min (pas 1 min)",
                                     "transactions", fill=True), use_container_width=True, theme=None)


with tab_live:
    st.fragment(run_every=4 if live else None)(overview)()


# =========================================================================== alertes
def alerts_tab() -> None:
    df = level_filter(load_alerts(minutes), min_level)
    f1, f2 = st.columns([2, 1])
    types = f1.multiselect("Typologies", list(viz.TYPE_LABEL), default=list(viz.TYPE_LABEL),
                           format_func=lambda t: viz.TYPE_LABEL[t])
    acc = f2.text_input("Compte (ex. ACC-0042)", "").strip().upper()
    if df.empty:
        st.info("Aucune alerte sur la période et au niveau sélectionnés.")
        return
    df = df[df["alert_type"].isin(types)]
    if acc:
        df = df[df["accounts"].apply(lambda a: acc in a)]
    table = pd.DataFrame({
        "Détectée": df["detected_at"].dt.tz_convert(data.DISPLAY_TZ).dt.strftime("%H:%M:%S"),
        "Niveau": df["risk_level"].map(viz.LEVEL_LABEL), "Score": df["risk_score"],
        "Typologie": df["alert_type"].map(viz.TYPE_LABEL),
        "Comptes": df["accounts"].apply(lambda a: " → ".join(a[:5]) + (" …" if len(a) > 5 else "")),
        "Montant (MAD)": df["total_amount"].round(0), "Latence (s)": (df["latency_ms"] / 1000).round(1),
        "Moteur": df["detector"], "ID": df["alert_id"]})
    st.dataframe(table, use_container_width=True, hide_index=True, height=260,
                 column_config={"Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100,
                                                                         format="%d")})
    if df.empty:
        return
    labels = {r.alert_id: f"{local(r.detected_at)} · {viz.LEVEL_LABEL[r.risk_level]} · "
                          f"{viz.TYPE_LABEL.get(r.alert_type)} · {', '.join(r.accounts[:3])}"
              for r in df.itertuples()}
    sel = st.selectbox("Alerte à investiguer", list(labels), format_func=labels.get)
    a = df[df["alert_id"] == sel].iloc[0]
    v, e = load_subgraphs((sel,))
    left, right = st.columns([3, 2])
    with left:
        if not e.empty:
            components.html(viz.network_html(e, v, height=440), height=450)
    with right:
        st.markdown(f"{badge(a.risk_level)} &nbsp;**{viz.TYPE_LABEL.get(a.alert_type)}** · "
                    f"score **{a.risk_score}**/100", unsafe_allow_html=True)
        st.markdown(a.explanation)
        m = st.columns(3)
        m[0].metric("Montant", viz.fmt_mad(a.total_amount))
        m[1].metric("Comptes", len(a.accounts))
        m[2].metric("Latence", f"{a.latency_ms / 1000:.1f} s" if pd.notna(a.latency_ms) else "—")
        with st.expander("Indicateurs du score (JSON)"):
            st.json(a.features)
        try:
            from fraud.report_pdf import investigation_pdf
            pdf = investigation_pdf(a.to_dict(), v, e)
            st.download_button("📄 Fiche d'investigation (PDF)", pdf, file_name=f"{a.alert_id}.pdf",
                               mime="application/pdf", use_container_width=True)
        except Exception as exc:
            st.caption(f"PDF indisponible : {exc}")
    if not e.empty:
        tx = e.sort_values("event_time")[["transaction_id", "src", "dst", "montant", "event_time", "type_operation"]]
        tx = tx.assign(event_time=data.to_local(tx["event_time"]).dt.strftime("%H:%M:%S"))
        st.dataframe(tx.rename(columns={"transaction_id": "Transaction", "src": "Émetteur", "dst": "Bénéficiaire",
                                        "montant": "Montant (MAD)", "event_time": "Heure",
                                        "type_operation": "Opération"}),
                     use_container_width=True, hide_index=True)


with tab_alerts:
    st.fragment(alerts_tab)()


# =========================================================================== explorateur
def network_tab() -> None:
    alerts = level_filter(load_alerts(minutes), "MEDIUM")
    default = ""
    if not alerts.empty:
        pivots = alerts[alerts["alert_type"].isin(["PIVOT", "MULE"])]
        default = (pivots.iloc[0] if not pivots.empty else alerts.iloc[0])["accounts"][0]
    c1, c2, c3 = st.columns([2, 1, 1])
    acc = c1.text_input("Compte à explorer", default).strip().upper()
    hops = c2.radio("Voisinage", [1, 2], horizontal=True, format_func=lambda h: f"{h} saut{'s' if h > 1 else ''}")
    c3.button("Actualiser", use_container_width=True)
    since = data.utcnow() - pd.Timedelta(minutes=minutes)
    if acc:
        e1 = data.account_edges(acc, since, limit=60)
        if e1.empty:
            st.info(f"Aucune transaction pour {acc} sur la période.")
        else:
            edges = e1
            if hops == 2:
                neigh = (set(e1["source_account"]) | set(e1["target_account"])) - {acc}
                more = [data.account_edges(n, since, limit=8) for n in list(neigh)[:15]]
                edges = pd.concat([e1, *more]).drop_duplicates("transaction_id")
            edges = edges.rename(columns={"source_account": "src", "target_account": "dst"})
            involved = alerts[alerts["accounts"].apply(lambda a: acc in a)] if not alerts.empty else alerts
            v = pd.DataFrame()
            if not involved.empty:
                v, _ = load_subgraphs(tuple(involved["alert_id"]))
            left, right = st.columns([3, 1])
            with left:
                components.html(viz.network_html(edges, v, height=520, focus=acc), height=530)
            with right:
                metrics = data.latest_metrics()
                row = metrics[metrics["account_id"] == acc] if not metrics.empty else pd.DataFrame()
                out = e1[e1["source_account"] == acc]["montant"].sum()
                inn = e1[e1["target_account"] == acc]["montant"].sum()
                st.metric("Reçu sur la période", viz.fmt_mad(inn))
                st.metric("Émis sur la période", viz.fmt_mad(out))
                st.metric("Taux de passage", f"{100 * out / inn:.0f} %" if inn else "—")
                if not row.empty and pd.notna(row.iloc[0]["pagerank_pct"]):
                    st.metric("PageRank (gros flux)", f"top {100 * (1 - row.iloc[0]['pagerank_pct']):.1f} %")
                st.metric("Alertes impliquant le compte", len(involved))
                for r in involved.head(4).itertuples():
                    st.markdown(f"{badge(r.risk_level)} {viz.TYPE_LABEL.get(r.alert_type)}", unsafe_allow_html=True)

    st.markdown("#### Centralité : PageRank du graphe des gros flux (≥ 20 000 MAD)")
    metrics = data.latest_metrics()
    if metrics.empty or metrics["pagerank"].isna().all():
        st.caption("Disponible après le premier passage du chemin lent (~30 s).")
        return
    top = metrics.dropna(subset=["pagerank"]).sort_values("pagerank", ascending=False).head(15)
    flagged = set(a for accs in alerts["accounts"] for a in accs) if not alerts.empty else set()
    colors = [viz.LEVEL_COLORS["CRITICAL"] if a in flagged else viz.SERIES[0] for a in top["account_id"]]
    names = [f"{a}  ⚠" if a in flagged else a for a in top["account_id"]]
    st.plotly_chart(viz.hbar(names, top["pagerank"].round(3).tolist(), None, colors, fmt=".3f",
                             x_title="PageRank"), use_container_width=True, theme=None)
    st.caption("⚠ rouge = compte impliqué dans une alerte · bleu = hub légitime (commerçant très actif). "
               "Un PageRank élevé seul ne suffit pas : il est combiné aux cycles pour désigner un pivot.")


with tab_net:
    st.fragment(network_tab)()


# =========================================================================== santé du pipeline
def health_tab() -> None:
    st.button("Actualiser les métriques", key="refresh_health")
    prog = load_metrics("pipeline_metrics.jsonl")
    fast = load_metrics("fast_path.jsonl")
    slow = load_metrics("slow_path.jsonl")
    if prog.empty:
        st.info("Aucune métrique : le job Spark n'a pas encore traité de micro-batch.")
        return
    last = prog.sort_values("ts").groupby("query").tail(1)
    st.dataframe(pd.DataFrame({
        "Requête": last["query"], "Dernier batch": last["batch_id"],
        "Entrée (lignes/s)": last["input_rps"].round(1), "Traitement (lignes/s)": last["processed_rps"].round(1),
        "Durée du trigger (ms)": last["trigger_ms"],
        "Watermark": last["watermark"].fillna("géré dans foreachBatch").where(last["query"] != "slow-path", "—"),
        "État (lignes)": last["state_rows"]}), hide_index=True, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        if not fast.empty:
            f = fast.tail(80).copy()
            parts = {"quality_ms": "Qualité + watermark", "window_ms": "Graphe glissant",
                     "detect_ms": "Motif finding", "publish_ms": "Alerte Kafka", "persist_ms": "Écriture Delta"}
            f = f.fillna({k: 0 for k in parts if k in f})
            parts = {k: v for k, v in parts.items() if k in f}
            long = f.melt(id_vars=["batch_id"], value_vars=list(parts), var_name="etape", value_name="n")
            st.plotly_chart(viz.stacked_bars(long, "batch_id", "etape", list(parts),
                                             dict(zip(parts, viz.SERIES)), parts,
                                             "Chemin rapide : durée par micro-batch (ms)", "ms"),
                            use_container_width=True, theme=None)
    with c2:
        if not fast.empty:
            lat = [x for xs in fast["alert_latency_ms"].dropna() for x in (xs or []) if x is not None]
            if lat:
                s = pd.Series(lat) / 1000
                st.plotly_chart(viz.histogram(s.tolist(), f"Latence des alertes carrousel — p50 {s.median():.2f} s · "
                                                          f"p95 {s.quantile(0.95):.2f} s", "secondes"),
                                use_container_width=True, theme=None)
    c3, c4 = st.columns(2)
    with c3:
        fp = prog[prog["query"] == "ingest"].tail(200)
        if not fp.empty:
            st.plotly_chart(viz.line(fp.assign(t=fp["ts"].dt.tz_convert(data.DISPLAY_TZ)), "t", "processed_rps",
                                     "Débit de traitement de l'ingestion", "lignes / s"),
                            use_container_width=True, theme=None)
    with c4:
        if not slow.empty:
            s = slow.iloc[-1]
            steps = {"pagerank_ms": "PageRank", "scc_ms": "SCC", "long_cycles_ms": "Cycles longs",
                     "mules_ms": "Mules", "scatter_ms": "Scatter-gather", "pivot_ms": "Pivots",
                     "clusters_ms": "Connected Components", "metrics_ms": "Métriques comptes"}
            vals = [(lbl, s.get(k)) for k, lbl in steps.items() if pd.notna(s.get(k))]
            st.plotly_chart(viz.hbar([x for x, _ in vals], [y for _, y in vals],
                                     f"Chemin lent : dernier passage ({s['window_edges']:,} arêtes".replace(",", " ")
                                     + f", {s['total_ms'] / 1000:.1f} s)", x_title="ms"),
                            use_container_width=True, theme=None)
    q = data.quarantine(data.utcnow() - pd.Timedelta(minutes=minutes))
    late = int(fast["late"].fillna(0).sum()) if "late" in fast else 0
    dup = int(fast["duplicates"].fillna(0).sum()) if "duplicates" in fast else 0
    k1, k2, k3 = st.columns(3)
    k1.metric("Événements trop tardifs écartés (watermark 2 min)", late)
    k2.metric("Doublons éliminés", dup)
    k3.metric("Messages invalides mis en quarantaine", len(q))
    if not q.empty:
        st.dataframe(q.groupby("error").size().reset_index(name="messages").rename(columns={"error": "Motif"}),
                     hide_index=True)


with tab_health:
    st.fragment(health_tab)()


# =========================================================================== audit delta
def audit_tab() -> None:
    from fraud.config import TABLES
    key = st.selectbox("Table", list(TABLES), index=list(TABLES).index("fraud_alerts"),
                       format_func=lambda k: TABLES[k])
    if not data.table_exists(key):
        st.info("Table pas encore créée.")
        return
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Organisation physique (partitions)**")
        st.dataframe(data.partition_layout(key), hide_index=True, use_container_width=True, height=260)
    with c2:
        st.markdown("**Historique des versions (journal de transactions Delta)**")
        h = data.history(key, 50)
        if not h.empty:
            cols = [c for c in ["version", "timestamp", "operation", "operationParameters"] if c in h]
            h = h[cols].assign(timestamp=data.to_local(h["timestamp"]).dt.strftime("%H:%M:%S"))
            st.dataframe(h, hide_index=True, use_container_width=True, height=260)
    if key == "fraud_alerts":
        st.markdown("#### Time travel : l'état des alertes à une version passée")
        t = data.delta(key)
        cur = t.version()
        if cur < 1:
            return
        ver = st.slider("Version", 0, cur, max(0, cur - 20))
        then = data.read(key, version=ver, columns=["alert_id", "risk_level", "alert_type"], days=2)
        now = data.read(key, columns=["alert_id", "risk_level", "alert_type"], days=2)
        comp = pd.DataFrame({
            f"Version {ver}": then.groupby("risk_level").size() if not then.empty else pd.Series(dtype=int),
            f"Actuelle ({cur})": now.groupby("risk_level").size() if not now.empty else pd.Series(dtype=int),
        }).reindex(viz.LEVEL_ORDER).fillna(0).astype(int)
        comp.index = comp.index.map(viz.LEVEL_LABEL)
        st.dataframe(comp, use_container_width=True)
        st.caption("La table gold est en `appendOnly` : aucune alerte ne peut être modifiée ou supprimée, "
                   "chaque version reste consultable — piste d'audit opposable.")


with tab_audit:
    st.fragment(audit_tab)()

if not live:
    st.sidebar.caption(f"Dernière mise à jour : {time.strftime('%H:%M:%S')}")
