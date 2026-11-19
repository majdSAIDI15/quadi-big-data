"""Génère toutes les figures de données du rapport (PDF vectoriels).

    py -3.10 rapport/generer_figures.py

Lit rapport/donnees/ (produit par exporter_donnees.py dans le conteneur) et écrit
rapport/figures/*.pdf. Quelques séries sont conservées en dur, faute de trace
exploitable dans le lakehouse (réinitialisé entre les runs) : l'historique des
optimisations et celui des runs d'évaluation, relevés pendant le développement
(voir l'annexe sur la reproductibilité).
"""
from __future__ import annotations

import json
import re
import sys
from datetime import timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import networkx as nx  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

RACINE = Path(__file__).resolve().parent.parent
D = RACINE / "rapport" / "donnees"
F = RACINE / "rapport" / "figures"
F.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(RACINE))
from fraud.scoring import score_cycle  # noqa: E402

BLEU, ORANGE = "#2a78d6", "#eb6834"
ENCRE, ENCRE2, GRILLE, GRIS = "#0b0b0b", "#52514e", "#e1e0d9", "#a9a8a1"
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
TYPES_SC = ["CARROUSEL", "PIVOT_HUB", "CYCLE_LONG", "SCATTER_GATHER", "FAN_IN_MULE"]
NOM_SC = {"CARROUSEL": "Carrousel", "PIVOT_HUB": "Pivot (3 carrousels)", "CYCLE_LONG": "Cycle long (4-5)",
          "SCATTER_GATHER": "Scatter-gather", "FAN_IN_MULE": "Compte mule"}
COUL_SC = dict(zip(TYPES_SC, CAT))
TYPES_AL = ["CYCLE", "PIVOT", "CLUSTER", "CYCLE_LONG", "SCATTER_GATHER", "MULE"]
NOM_AL = {"CYCLE": "Carrousel", "CYCLE_LONG": "Cycle long", "MULE": "Mule", "SCATTER_GATHER": "Scatter-gather",
          "PIVOT": "Pivot", "CLUSTER": "Réseau (CC)"}
COUL_AL = dict(zip(TYPES_AL, CAT))
LARGEUR = 6.3

plt.rcParams.update({
    "font.family": "sans-serif", "font.sans-serif": ["Segoe UI", "DejaVu Sans"], "font.size": 8.5,
    "text.color": ENCRE, "axes.edgecolor": GRILLE, "axes.labelcolor": ENCRE2, "xtick.color": ENCRE2,
    "ytick.color": ENCRE2, "axes.titlesize": 9, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "legend.frameon": False, "legend.fontsize": 8, "figure.facecolor": "white", "axes.facecolor": "white",
    "savefig.bbox": "tight", "savefig.pad_inches": 0.03, "pdf.fonttype": 42,
})


def sans_cadre(ax, cotes=("top", "right")):
    for c in cotes:
        ax.spines[c].set_visible(False)
    ax.tick_params(length=0)


def grille(ax, axe="y"):
    ax.grid(axis=axe, color=GRILLE, linewidth=0.6)
    ax.set_axisbelow(True)


def fr(x, d=0):
    s = f"{x:,.{d}f}".replace(",", " ").replace(".", ",")
    return s


def sauver(fig, nom):
    fig.savefig(F / f"{nom}.pdf")
    plt.close(fig)
    print("figure", nom)


def lire_jsonl(nom):
    p = D / nom
    rows = [json.loads(line) for line in open(p, encoding="utf-8") if line.strip()] if p.exists() else []
    df = pd.DataFrame(rows)
    if "ts" in df:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")
    return df


FEN = json.load(open(D / "fenetres.json"))
RUN0, RUN1 = pd.Timestamp(FEN["run_debut"]), pd.Timestamp(FEN["run_fin"])
GT = pd.read_json(D / "verite_terrain.json")
GT["injected_at"] = pd.to_datetime(GT["injected_at"], utc=True)
AL = pd.read_csv(D / "alertes.csv")
AL["detected_at"] = pd.to_datetime(AL["detected_at"], utc=True)
AL["accounts"] = AL["accounts"].fillna("").str.split(";")
AL["transaction_ids"] = AL["transaction_ids"].fillna("").str.split(";")
EVAL0 = GT[GT["injected_at"] >= RUN0]["injected_at"].min() - pd.Timedelta(seconds=30)
AL_RUN = AL[(AL["detected_at"] >= EVAL0) & (AL["detected_at"] <= RUN1)].copy()
FAST = lire_jsonl("fast_path.jsonl")
SLOW = lire_jsonl("slow_path.jsonl")
RES = json.load(open(D / "resultats.json", encoding="utf-8"))
VITRINE = json.load(open(D / "vitrine_alertes.json", encoding="utf-8"))


def minutes_depuis(ts, t0=RUN0):
    return (ts - t0).dt.total_seconds() / 60


# =========================================================================== CH. 4 — données
def fig_trafic():
    t = pd.read_csv(D / "trafic_minute.csv", parse_dates=["minute"])
    t = t[(t["minute"] >= RUN0.ceil("min")) & (t["minute"] < RUN1.floor("min"))]
    x = minutes_depuis(t["minute"])
    gt = GT[(GT["injected_at"] >= RUN0) & (GT["injected_at"] <= RUN1)]
    chauffe = float(minutes_depuis(pd.Series([EVAL0 + pd.Timedelta(seconds=30)])).iloc[0])
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(LARGEUR, 3.0), sharex=True,
                                 gridspec_kw={"height_ratios": [2.3, 1], "hspace": 0.12})
    a1.fill_between(x, t["tx"], color=BLEU, alpha=0.15, linewidth=0)
    a1.plot(x, t["tx"], color=BLEU, linewidth=1.6)
    a1.axvspan(0, chauffe, color=GRILLE, alpha=0.45, linewidth=0)
    a1.text(chauffe / 2, t["tx"].max() * 0.55, "chauffe :\ntrafic légitime seul\n(historique des comptes)",
            ha="center", fontsize=7.5, color=ENCRE2)
    a1.set_ylabel("transactions / min")
    a1.set_ylim(0, t["tx"].max() * 1.15)
    sans_cadre(a1)
    grille(a1)
    for k, typ in enumerate(TYPES_SC):
        s = gt[gt["scenario_type"] == typ]
        a2.scatter(minutes_depuis(s["injected_at"]), np.full(len(s), k), s=16, color=COUL_SC[typ], zorder=3)
    a2.set_yticks(range(len(TYPES_SC)), [NOM_SC[t] for t in TYPES_SC], fontsize=7)
    a2.set_ylim(-0.7, len(TYPES_SC) - 0.3)
    a2.invert_yaxis()
    a2.set_xlabel("minutes depuis le démarrage du run")
    sans_cadre(a2)
    grille(a2, "x")
    a1.set_title(f"Run d'évaluation final : {fr(t['tx'].sum())} transactions, {len(gt)} fraudes injectées")
    sauver(fig, "trafic_run")


def fig_activite():
    a = pd.read_csv(D / "activite_comptes.csv")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(LARGEUR, 2.4), gridspec_kw={"wspace": 0.3})
    for col, coul, nom in (("emises", BLEU, "émises"), ("recues", ORANGE, "reçues")):
        v = np.sort(a[col].to_numpy())[::-1]
        a1.loglog(np.arange(1, len(v) + 1), np.maximum(v, 0.8), color=coul, linewidth=1.6, label=f"transactions {nom}")
    a1.set_xlabel("rang du compte (du plus actif au moins actif)")
    a1.set_ylabel("transactions sur le run")
    a1.legend(loc="lower left")
    a1.set_title("Activité très inégale (loi de puissance)")
    sans_cadre(a1)
    grille(a1, "both")
    top = a.sort_values("recues", ascending=False)
    part = top["recues"].head(200).sum() / top["recues"].sum()
    a2.bar(["2 % des comptes\nles plus payés", "98 % restants"], [part * 100, (1 - part) * 100],
           color=[ORANGE, GRIS], width=0.55)
    for i, v in enumerate([part * 100, (1 - part) * 100]):
        a2.text(i, v + 1.5, f"{fr(v)} %", ha="center", fontsize=8.5, fontweight="bold")
    a2.set_ylabel("part des paiements reçus (%)")
    a2.set_ylim(0, 100)
    a2.set_title("Les commerçants concentrent les flux")
    sans_cadre(a2)
    grille(a2)
    sauver(fig, "activite_comptes")


def fig_montants():
    m = pd.read_csv(D / "montants_echantillon.csv")
    noms = {"VIREMENT_COMMERCIAL": "virement commercial", "PAIEMENT_FOURNISSEUR": "paiement fournisseur",
            "REGLEMENT_FACTURE": "règlement de facture", "TRANSFERT_INTERNE": "transfert interne",
            "VIREMENT_PARTICULIER": "virement entre particuliers"}
    ordre = m.groupby("type_operation")["montant"].median().sort_values().index.tolist()
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.3))
    bp = ax.boxplot([m[m["type_operation"] == t]["montant"] for t in ordre], vert=False, widths=0.55,
                    showfliers=False, patch_artist=True, whis=(5, 95),
                    medianprops={"color": ENCRE, "linewidth": 1.4}, whiskerprops={"color": GRIS},
                    capprops={"color": GRIS})
    for b in bp["boxes"]:
        b.set(facecolor="#dceafb", edgecolor=BLEU)
    ax.set_yticks(range(1, len(ordre) + 1), [noms.get(o, o.lower()) for o in ordre], fontsize=7.5)
    ax.set_xscale("log")
    ax.axvline(20_000, color=ORANGE, linewidth=1.2, linestyle="--")
    ax.text(21_500, 0.55, "seuil des « gros flux »\n20 000 MAD", color=ORANGE, fontsize=7.5)
    ax.set_xlabel("montant (MAD, échelle logarithmique) — boîte : quartiles, moustaches : 5e et 95e centiles")
    sans_cadre(ax)
    grille(ax, "x")
    sauver(fig, "montants_types")


def fig_scenarios():
    tx = pd.read_csv(D / "anomaly_subgraph_edges.csv")
    tx["event_time"] = pd.to_datetime(tx["event_time"], utc=True, format="ISO8601")
    ids = set(tx["transaction_id"])
    fig, axes = plt.subplots(1, 5, figsize=(LARGEUR, 1.9), gridspec_kw={"wspace": 0.45})
    for ax, typ in zip(axes, TYPES_SC):
        cand = GT[(GT["scenario_type"] == typ) & (GT["injected_at"] >= RUN0)]
        cand = cand[cand["transaction_ids"].apply(lambda t: set(t) <= ids)] if len(cand) else cand
        if cand.empty:
            ax.axis("off")
            continue
        g = cand.iloc[len(cand) // 2]
        e = tx[tx["transaction_id"].isin(g["transaction_ids"])].drop_duplicates("transaction_id").sort_values("event_time")
        t = (e["event_time"] - e["event_time"].min()).dt.total_seconds()
        coul = [ORANGE if (typ == "FAN_IN_MULE" and s == g["key_accounts"][0]) else COUL_SC[typ] for s in e["src"]]
        ax.scatter(t, e["montant"] / 1000, s=14, color=coul, zorder=3)
        ax.plot(t, e["montant"] / 1000, color=GRIS, linewidth=0.6, zorder=2)
        ax.set_title(NOM_SC[typ], fontsize=7.5)
        ax.set_xlabel("s depuis le 1er virement", fontsize=6.5)
        ax.tick_params(labelsize=6.5)
        sans_cadre(ax)
        grille(ax)
    axes[0].set_ylabel("montant (k MAD)", fontsize=7)
    sauver(fig, "scenarios_chronologie")


def fig_qualite():
    r = pd.read_csv(D / "retards_evenements.csv")["retard_evenement_s"]
    q = pd.read_csv(D / "transactions_quarantine.csv")
    q["kafka_timestamp"] = pd.to_datetime(q["kafka_timestamp"], utc=True, format="ISO8601")
    q = q[(q["kafka_timestamp"] >= RUN0) & (q["kafka_timestamp"] <= RUN1)]
    f = FAST[(FAST["ts"] >= RUN0) & (FAST["ts"] <= RUN1)]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(LARGEUR, 2.3), gridspec_kw={"wspace": 0.35, "width_ratios": [1.4, 1]})
    retard = r[(r > 5) & (r <= 130)]
    a1.hist(retard, bins=np.arange(0, 135, 5), color=BLEU, edgecolor="white")
    a1.axvline(120, color=ORANGE, linewidth=1.3, linestyle="--")
    a1.text(118, a1.get_ylim()[1] * 0.85, "watermark\n2 min", color=ORANGE, fontsize=7.5, ha="right")
    a1.set_xlabel("retard de l'événement à l'ingestion (s)")
    a1.set_ylabel("transactions acceptées")
    a1.set_title(f"{fr(len(retard))} événements en retard acceptés")
    sans_cadre(a1)
    grille(a1)
    cats = {"en retard > 2 min\n(écartés)": int(f["late"].sum()), "doublons\néliminés": int(f["duplicates"].sum())}
    for err, n in q["error"].value_counts().items():
        cats[err.replace(" ou ", "\nou ")] = int(n)
    a2.barh(list(cats), list(cats.values()), color=[ORANGE, GRIS] + [BLEU] * (len(cats) - 2), height=0.6)
    for i, v in enumerate(cats.values()):
        a2.text(v + 0.3, i, str(v), va="center", fontsize=7.5)
    a2.invert_yaxis()
    a2.tick_params(labelsize=7)
    a2.set_title("Rejets du contrôle qualité")
    sans_cadre(a2)
    grille(a2, "x")
    sauver(fig, "qualite_donnees")


# =========================================================================== CH. 3/8 — micro-batch
def fig_etapes():
    f = FAST[(FAST["ts"] >= RUN0) & (FAST["ts"] <= RUN1) & (FAST["fresh"] > 0)]
    etapes = {"quality_ms": "qualité + watermark", "window_ms": "graphe glissant (index)",
              "detect_ms": "motif GraphFrames", "publish_ms": "alerte Kafka", "persist_ms": "écritures Delta"}
    med = {v: f[k].median() for k, v in etapes.items()}
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(LARGEUR, 2.3), gridspec_kw={"wspace": 0.6, "width_ratios": [1.25, 1]})
    noms = list(med)[::-1]
    a1.barh(noms, [max(med[n], 1) for n in noms],
            color=[ORANGE if n == "motif GraphFrames" else BLEU for n in noms], height=0.6)
    for i, n in enumerate(noms):
        v = med[n]
        a1.text(max(v, 1) * 1.15, i, f"{fr(v / 1000, 2)} s" if v >= 100 else (f"{fr(v)} ms" if v >= 1 else "< 1 ms"), va="center", fontsize=7.5)
    a1.set_xscale("log")
    a1.set_xlim(0.8, 20_000)
    a1.set_xlabel("durée médiane par micro-batch (ms, log)")
    a1.set_title("Où passe le temps d'un micro-batch")
    sans_cadre(a1)
    grille(a1, "x")
    tot = f["total_ms"] / 1000
    a2.hist(tot.clip(upper=15), bins=30, color=BLEU, edgecolor="white")
    a2.axvline(tot.median(), color=ENCRE, linewidth=1)
    a2.text(tot.median() + 0.4, a2.get_ylim()[1] * 0.85, f"médiane {fr(tot.median(), 1)} s", fontsize=7.5)
    a2.set_xlabel("durée totale d'un micro-batch (s)")
    a2.set_title(f"{fr(len(f))} micro-batchs du run")
    sans_cadre(a2)
    grille(a2)
    sauver(fig, "etapes_microbatch")


def fig_fenetrage():
    tw = pd.read_csv(D / "traffic_windows.csv", parse_dates=["window_start", "window_end"])
    tw = tw[(tw["window_start"] >= EVAL0) & (tw["window_end"] <= RUN1)].sort_values("window_start").head(10)
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.2))
    x0 = minutes_depuis(tw["window_start"]).to_numpy()
    x1 = minutes_depuis(tw["window_end"]).to_numpy()
    for k, (a, b, n) in enumerate(zip(x0, x1, tw["tx_count"])):
        ax.barh(k, b - a, left=a, height=0.62, color=BLEU if k % 2 == 0 else "#7fb0ea")
        ax.text(b + 0.25, k, f"{fr(n)} tx", va="center", fontsize=7)
    ax.set_yticks(range(len(tw)), [f"fenêtre {k + 1}" for k in range(len(tw))], fontsize=7)
    ax.invert_yaxis()
    ax.set_xlabel("minutes depuis le démarrage du run")
    ax.set_xlim(x0.min() - 0.5, x1.max() + 3.2)
    ax.set_title("window(10 min, 1 min) : chaque transaction compte dans 10 fenêtres successives")
    sans_cadre(ax)
    grille(ax, "x")
    sauver(fig, "fenetres_glissantes")


# =========================================================================== CH. 5 — algorithmes
def fig_elagage():
    e = pd.read_csv(D / "elagage_candidats.csv")
    e = e[e["fenetre"] > 20_000]
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.0))
    vals = [e["fenetre"].median(), e["nouvelles"].median(), e["candidates"].median()]
    noms = ["arêtes du graphe glissant\n(10 + 2 min)", "arêtes nouvelles du lot\n(2 s)", "candidates au motif\n(après élagage)"]
    ax.barh(noms[::-1], vals[::-1], color=[ORANGE, GRIS, BLEU], height=0.55)
    ax.set_xscale("log")
    for i, v in enumerate(vals[::-1]):
        ax.text(v * 1.12, i, fr(v), va="center", fontsize=8.5, fontweight="bold")
    ax.set_xlim(1, vals[0] * 6)
    ax.set_xlabel("nombre d'arêtes (médiane sur les lots du run, échelle logarithmique)")
    ax.set_title(f"Le motif ne voit que {fr(100 * vals[2] / vals[0], 1)} % du graphe glissant")
    sans_cadre(ax)
    grille(ax, "x")
    sauver(fig, "elagage_candidats")


def fig_score():
    a = VITRINE["CYCLE"]
    e = sorted(a["aretes"], key=lambda r: r["event_time"])
    edges = [{"src": r["src"], "dst": r["dst"], "montant": r["montant"], "type_operation": r["type_operation"],
              "event_time": pd.Timestamp(r["event_time"]).to_pydatetime(), "transaction_id": r["transaction_id"]}
             for r in e]
    sc = score_cycle(edges, {})
    regles = [("quasi identiques", "montants quasi\nidentiques"), ("proches", "montants\nproches"),
              ("perte régulière", "commission\nrégulière"), ("bouclée", "boucle\nrapide"),
              ("élevés", "montants\nélevés"), ("significatifs", "montants\nsignificatifs"),
              ("supérieurs", "montants\n> 10 000"), ("même type", "même type\nd'opération"),
              ("cycle long", "cycle\nlong")]
    pts = []
    for r in sc.reasons:
        p = int(re.search(r"\(\+(\d+)\)$", r).group(1))
        nom = next((lab for cle, lab in regles if cle in r), r[:20])
        pts.append((nom, p))
    total = int(a["alerte"]["risk_score"])
    if total > sc.score:
        pts.append(("PageRank\nélevé", total - sc.score))
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.2))
    cumul = 0
    for i, (nom, p) in enumerate(pts):
        ax.bar(i, p, bottom=cumul, color=BLEU, width=0.6)
        ax.text(i, cumul + p + 1.5, f"+{p}", ha="center", fontsize=8, fontweight="bold")
        cumul += p
    ax.bar(len(pts), cumul, color=ORANGE, width=0.6)
    ax.text(len(pts), cumul + 1.5, str(cumul), ha="center", fontsize=8.5, fontweight="bold")
    for y, lvl in ((40, "MEDIUM"), (70, "HIGH"), (90, "CRITICAL")):
        ax.axhline(y, color=GRIS, linewidth=0.7, linestyle=":")
        ax.text(len(pts) + 0.45, y, lvl, va="center", fontsize=6.5, color=ENCRE2)
    ax.set_xticks(range(len(pts) + 1), [n for n, _ in pts] + ["score\nfinal"], fontsize=7)
    ax.set_ylim(0, 108)
    ax.set_ylabel("points")
    ax.set_title("Anatomie du score d'une alerte carrousel réelle (niveau CRITICAL)")
    sans_cadre(ax)
    sauver(fig, "score_carrousel")


def verites(al):
    tx = [set(t) for t in GT["transaction_ids"]]
    keys = [set(k) for k in GT["key_accounts"]]

    def vrai(a):
        s = set(a["transaction_ids"])
        if any(s & t for t in tx):
            return True
        return a["alert_type"] in ("MULE", "PIVOT", "SCATTER_GATHER", "CLUSTER") and any(set(a["accounts"]) & k for k in keys)
    return al.apply(vrai, axis=1)


def fig_scores_types():
    al = AL_RUN.copy()
    al["vrai"] = verites(al)
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.5))
    for b0, b1, nom, c in ((20, 40, "LOW", "#f4f4f1"), (40, 70, "MEDIUM", "#fdf3d6"), (70, 90, "HIGH", "#fce3d8"),
                           (90, 101, "CRITICAL", "#f8d0cf")):
        ax.axhspan(b0, b1, color=c, linewidth=0)
        ax.text(len(TYPES_AL) - 0.45, (b0 + b1) / 2, nom, fontsize=6.5, color=ENCRE2, va="center")
    rng = np.random.default_rng(3)
    for i, t in enumerate(TYPES_AL):
        s = al[al["alert_type"] == t]
        x = i + rng.uniform(-0.22, 0.22, len(s))
        v = s["vrai"].to_numpy()
        ax.scatter(x[v], s["risk_score"][v], s=12, color=COUL_AL[t], zorder=3)
        ax.scatter(x[~v], s["risk_score"][~v], s=24, facecolor="white", edgecolor=ENCRE, linewidth=1,
                   marker="X", zorder=4)
    h1 = plt.Line2D([], [], marker="o", linestyle="", color=GRIS, markersize=4, label="alerte vraie (fraude injectée)")
    h2 = plt.Line2D([], [], marker="X", linestyle="", markerfacecolor="white", markeredgecolor=ENCRE, markersize=6,
                    label="alerte sans fraude derrière")
    ax.legend(handles=[h1, h2], loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2, fontsize=7.5)
    ax.set_xticks(range(len(TYPES_AL)), [NOM_AL[t] for t in TYPES_AL], fontsize=7.5)
    ax.set_ylim(18, 103)
    ax.set_xlim(-0.6, len(TYPES_AL) - 0.1)
    ax.set_ylabel("score de risque")
    ax.set_title(f"Scores des {len(al)} alertes de la période évaluée")
    sans_cadre(ax)
    sauver(fig, "scores_par_type")


def fig_gros_flux():
    g = pd.read_csv(D / "elagage_gros_flux.csv")
    med = g[["aretes_fenetre", "aretes_gros_flux", "aretes_noyau"]].median()
    noms = ["toutes les arêtes\nde la fenêtre", "gros flux\n(≥ 20 000 MAD)", "noyau après élagage\n(4 tours)"]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(LARGEUR, 2.2), gridspec_kw={"wspace": 0.6, "width_ratios": [1.5, 1]})
    a1.barh(noms[::-1], med.to_numpy()[::-1], color=[BLEU, GRIS, GRIS], height=0.55)
    a1.set_xscale("log")
    for i, v in enumerate(med.to_numpy()[::-1]):
        a1.text(v * 1.15, i, fr(v), va="center", fontsize=8.5, fontweight="bold")
    a1.set_xlim(1, med.max() * 8)
    a1.set_title("Ce que la SCC doit vraiment traiter")
    a1.set_xlabel("arêtes (médiane des fenêtres, log)")
    sans_cadre(a1)
    grille(a1, "x")
    t = minutes_depuis(pd.to_datetime(g["t"], utc=True, format="ISO8601"))
    a2.plot(t, g["aretes_noyau"], color=BLEU, linewidth=1.5, marker="o", markersize=3, label="arêtes du noyau")
    a2.bar(t, g["scc_4plus"], width=1.2, color=ORANGE, label="SCC ≥ 4 comptes")
    a2.set_xlabel("minutes du run")
    a2.set_title("Au fil du run")
    a2.legend(fontsize=6.8, loc="upper left")
    sans_cadre(a2)
    grille(a2)
    sauver(fig, "elagage_gros_flux")


def fig_pagerank():
    from matplotlib.patches import Patch
    m = pd.read_csv(D / "account_metrics.csv", parse_dates=["snapshot_time"])
    m = m[m["snapshot_time"] <= RUN1]
    snap = m[m["snapshot_time"] == m["snapshot_time"].max()].dropna(subset=["pagerank"])
    top = snap.sort_values("pagerank", ascending=False).head(15)
    alertes = set(a for accs in AL[AL["risk_level"] != "LOW"]["accounts"] for a in accs)
    coul = [ORANGE if a in alertes else BLEU for a in top["account_id"]]
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.6))
    ax.barh(top["account_id"][::-1], top["pagerank"][::-1], color=coul[::-1], height=0.65)
    ax.set_xlabel("PageRank sur le graphe des gros flux (fenêtre de 10 min)")
    ax.tick_params(axis="y", labelsize=7)
    ax.legend(handles=[Patch(color=ORANGE, label="compte impliqué dans une alerte"),
                       Patch(color=BLEU, label="hub légitime (aucune alerte)")], loc="lower right")
    n = sum(c == ORANGE for c in coul)
    ax.set_title(f"Top 15 du PageRank : {n} comptes alertés, {15 - n} hubs légitimes")
    sans_cadre(ax)
    grille(ax, "x")
    sauver(fig, "pagerank_top")


def fig_mule():
    v = json.load(open(D / "mule_vs_commercant.json"))
    fig, axes = plt.subplots(2, 1, figsize=(LARGEUR, 3.0), sharex=False, gridspec_kw={"hspace": 0.75})
    if "mule" not in v:
        return
    mu = v["mule"]
    so = max(mu["sorties"], key=lambda r: r["montant"])
    ts = pd.Timestamp(so["t"])
    for ax, cle, coul in ((axes[0], "commercant", BLEU), (axes[1], "mule", ORANGE)):
        d = pd.DataFrame(v[cle]["depots"])
        d["t"] = pd.to_datetime(d["t"], utc=True, format="ISO8601")
        x = (d["t"] - ts).dt.total_seconds() / 60
        sel = (x >= -26) & (x <= 4)
        d, x = d[sel], x[sel]
        rafale = ((x > -6) & (x <= 0)).sum()
        habituel = ((x > -26) & (x <= -6)).sum()
        ax.axvspan(-26, -6, color=GRILLE, alpha=0.6, linewidth=0)
        ax.axvspan(-6, 0, color=ORANGE, alpha=0.13, linewidth=0)
        ax.vlines(x, 0, d["montant"] / 1000, color=coul, linewidth=1.4)
        ax.scatter(x, d["montant"] / 1000, s=10, color=coul, zorder=3)
        ymax = max(d["montant"].max() / 1000, 1) * 1.45
        if cle == "mule":
            ax.vlines(0, 0, so["montant"] / 1000, color=ENCRE, linewidth=2)
            ax.text(0.3, ymax * 0.8, f"retrait unique\n{fr(so['montant'])} MAD", fontsize=7)
            ymax = max(ymax, so["montant"] / 1000 * 1.1)
        base = habituel / 20 * 6
        ax.text(-16, ymax * 0.86, f"{habituel} dépôt{'s' if habituel > 1 else ''} en 20 min\n(rythme habituel : {fr(base, 1)} par 6 min)",
                ha="center", fontsize=6.8, color=ENCRE2)
        fac = rafale / max(base, 1)
        ax.text(-3, ymax * 0.86, f"{rafale} dépôts en 6 min\n× {fr(fac, 1)}", ha="center", fontsize=6.8,
                color=ORANGE if fac >= 3 else ENCRE2, fontweight="bold")
        ax.set_ylim(0, ymax)
        ax.set_xlim(-26.5, 4.5)
        ax.set_ylabel("dépôt (k MAD)", fontsize=7)
        titre = (f"Commerçant {v[cle]['compte']} : beaucoup de dépôts, mais toujours au même rythme → pas d'alerte"
                 if cle == "commercant" else
                 f"Mule {v[cle]['compte']} : compte calme, rafale de petits dépôts puis vidage → alerte MULE")
        ax.set_title(titre, fontsize=8)
        sans_cadre(ax)
        grille(ax)
    axes[1].set_xlabel("minutes avant / après le plus gros virement sortant de la mule")
    axes[0].set_xlabel("mêmes fenêtres appliquées au commerçant", fontsize=7)
    sauver(fig, "mule_velocite")


def fig_reseau():
    cle = "CLUSTER" if "CLUSTER" in VITRINE else "PIVOT"
    a = VITRINE[cle]
    roles = {s["account_id"]: s["role"] for s in a["sommets"]}
    g = nx.MultiDiGraph()
    for e in a["aretes"]:
        g.add_edge(e["src"], e["dst"], montant=e["montant"])
    pivot = [n for n, r in roles.items() if r == "PIVOT"]
    pos = nx.spring_layout(nx.Graph(g), seed=7, k=1.1)
    fig, ax = plt.subplots(figsize=(LARGEUR * 0.75, 3.0))
    nx.draw_networkx_edges(g, pos, ax=ax, edge_color=GRIS, width=1.2, arrows=True, arrowsize=11,
                           connectionstyle="arc3,rad=0.12", node_size=900)
    nx.draw_networkx_nodes(g, pos, ax=ax, node_color=[ORANGE if n in pivot else BLEU for n in g.nodes],
                           node_size=[1100 if n in pivot else 750 for n in g.nodes], edgecolors="white", linewidths=1.5)
    nx.draw_networkx_labels(g, pos, ax=ax, labels={n: n.replace("ACC-", "") for n in g.nodes},
                            font_size=7, font_color="white", font_weight="bold")
    lab = {}
    for u, v, d in g.edges(data=True):
        lab[(u, v)] = f"{d['montant'] / 1000:.0f}k"
    nx.draw_networkx_edge_labels(g, pos, edge_labels=lab, ax=ax, font_size=6.5, font_color=ENCRE2,
                                 bbox={"boxstyle": "round,pad=0.1", "fc": "white", "ec": "none"})
    ax.set_title(f"Alerte {cle} réelle : {len(g.nodes)} comptes, {g.number_of_edges()} virements (k MAD)", fontsize=8.5)
    ax.axis("off")
    sauver(fig, "reseau_cluster")


# =========================================================================== CH. 6 — Delta
def fig_delta():
    h = pd.read_csv(D / "delta_historique.csv")
    h["timestamp"] = pd.to_datetime(h["timestamp"], utc=True, format="ISO8601")
    p = pd.read_csv(D / "delta_partitions.csv")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(LARGEUR, 2.4), gridspec_kw={"wspace": 0.45, "width_ratios": [1.4, 1]})
    tables = {"bronze/transactions": BLEU, "gold/fraud_alerts": ORANGE, "silver/account_metrics": CAT[2],
              "silver/traffic_windows": CAT[3]}
    for t, c in tables.items():
        s = h[h["table"] == t].sort_values("timestamp")
        s = s[(s["timestamp"] >= RUN0) & (s["timestamp"] <= RUN1)]
        a1.plot(minutes_depuis(s["timestamp"]), s["version"] - s["version"].min(), color=c, linewidth=1.6,
                label=t.split("/")[1].replace("_", " "))
        opt = s[s["operation"] == "OPTIMIZE"]
        a1.scatter(minutes_depuis(opt["timestamp"]), opt["version"] - s["version"].min(), color=ENCRE, s=12, zorder=4)
    a1.scatter([], [], color=ENCRE, s=12, label="OPTIMIZE (compactage)")
    a1.set_xlabel("minutes du run")
    a1.set_ylabel("commits Delta")
    a1.legend(fontsize=6.8)
    a1.set_title("Versions des tables pendant le run")
    sans_cadre(a1)
    grille(a1)
    g = p[p["table"] == "gold/fraud_alerts"].groupby("risk_level")["lignes"].sum().reindex(
        ["LOW", "MEDIUM", "HIGH", "CRITICAL"]).fillna(0)
    a2.bar(g.index, g.values, color=["#a9a8a1", "#eda100", ORANGE, "#d03b3b"], width=0.6)
    for i, v in enumerate(g.values):
        a2.text(i, v + 1, fr(v), ha="center", fontsize=8)
    a2.set_title("gold/fraud_alerts : lignes\npar partition risk_level")
    sans_cadre(a2)
    grille(a2)
    sauver(fig, "delta_tables")


def fig_panne():
    if "panne_kill" not in FEN:
        return
    k, r = pd.Timestamp(FEN["panne_kill"]), pd.Timestamp(FEN["panne_restart"])
    d0, d1 = pd.Timestamp(FEN["panne_debut"]), pd.Timestamp(FEN["panne_fin"])
    f = FAST[(FAST["ts"] >= d0) & (FAST["ts"] <= d1)]
    x = (f["ts"] - k).dt.total_seconds()
    apres = f[f["ts"] > r]
    reprise = (apres["ts"].iloc[0] - k).total_seconds() if len(apres) else (r - k).total_seconds()
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.1))
    ax.bar(x, f["rows"], width=3, color=BLEU)
    ax.axvspan(0, (r - k).total_seconds(), color=ORANGE, alpha=0.16, linewidth=0)
    ax.axvspan((r - k).total_seconds(), reprise, color=GRILLE, alpha=0.7, linewidth=0)
    ax.axvline(0, color=ORANGE, linewidth=1.3)
    top = f["rows"].max() * 1.25
    ax.set_ylim(0, top)
    ax.text(3, top * 0.9, "kill -9", color=ORANGE, fontsize=8, fontweight="bold")
    ax.text((r - k).total_seconds() / 2, top * 0.72, "Spark\narrêté", ha="center", fontsize=7, color=ORANGE)
    ax.text(((r - k).total_seconds() + reprise) / 2, top * 0.3,
            "redémarrage : JVM, lecture des\ncheckpoints, état reconstruit\ndepuis le bronze", ha="center",
            fontsize=7, color=ENCRE2)
    if len(apres):
        j = apres.loc[apres["rows"].idxmax()]
        ax.annotate(f"rattrapage : {fr(j['rows'])} lignes\nen un seul lot", xy=((j["ts"] - k).total_seconds(), j["rows"]),
                    xytext=(-105, 2), textcoords="offset points", fontsize=7.5,
                    arrowprops={"arrowstyle": "-", "color": ENCRE2})
    ax.set_ylabel("lignes par micro-batch")
    ax.set_xlabel("secondes depuis la panne")
    ax.set_title("Arrêt brutal de Spark en pleine ingestion, puis reprise sur checkpoint")
    sans_cadre(ax)
    grille(ax)
    sauver(fig, "panne_reprise")


# =========================================================================== CH. 8 — performance
def lire_memoire(nom):
    rows = []
    for line in open(D / nom, encoding="utf-8"):
        m = re.match(r"(\d\d):(\d\d) ([\d.]+)(MiB|GiB)", line.strip())
        if m:
            v = float(m.group(3)) / (1024 if m.group(4) == "MiB" else 1)
            rows.append((int(m.group(1)) * 60 + int(m.group(2)), v))
    df = pd.DataFrame(rows, columns=["min", "go"])
    df["min"] -= df["min"].iloc[0]
    return df


def fig_memoire():
    a = lire_memoire("memoire_run_fuite.txt")
    b = lire_memoire("memoire_run_final.txt")
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.2))
    ax.plot(a["min"], a["go"], color=ORANGE, linewidth=1.7, label="avant : mémoire native non bornée (limite 3,5 Go)")
    ax.plot(b["min"], b["go"], color=BLEU, linewidth=1.7, label="final : MALLOC_ARENA_MAX=2, MaxDirectMemorySize")
    ax.axhline(3.5, color=ORANGE, linewidth=0.8, linestyle="--")
    ax.axhline(4.0, color=BLEU, linewidth=0.8, linestyle="--")
    ax.text(0.5, 3.53, "limite 3,5 Go", fontsize=7, color=ORANGE)
    ax.text(0.5, 4.03, "limite 4 Go", fontsize=7, color=BLEU)
    ax.set_ylim(0, 4.4)
    ax.set_xlabel("minutes de fonctionnement")
    ax.set_ylabel("mémoire du conteneur\nSpark (Go)")
    ax.legend(loc="lower right", fontsize=7)
    ax.set_title("Stabilité mémoire sur 40 minutes de streaming")
    sans_cadre(ax)
    grille(ax)
    sauver(fig, "memoire")


def fig_optimisations():
    v = [("V1\nmotif sur toute\nla fenêtre", 97.1, 68.7),
         ("V2\nsous-graphe\nmatérialisé", 15.0, 11.0),
         ("V3\n3 requêtes,\nchemin lent bridé", 13.9, 6.5),
         ("V3b\nélagage\npar isin()", 30.2, 16.8),
         ("V4\nindex en\nmémoire + Arrow", 4.1, 0.81),
         ("V5\nlibération\nmémoire", 3.4, 0.73)]
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.6))
    x = np.arange(len(v))
    ax.plot(x, [b for _, b, _ in v], color=BLEU, marker="o", linewidth=1.8, label="durée d'un micro-batch")
    ax.plot(x, [m for _, _, m in v], color=ORANGE, marker="o", linewidth=1.8, label="dont motif GraphFrames")
    for i, (_, b, m) in enumerate(v):
        ax.text(i, b * 1.35, f"{fr(b, 1)} s", ha="center", fontsize=7.5, color=BLEU, fontweight="bold")
        ax.text(i, m / 1.75, f"{fr(m, 2) if m < 1 else fr(m, 1)} s", ha="center", fontsize=7.5, color=ORANGE)
    ax.set_yscale("log")
    ax.set_ylim(0.3, 250)
    ax.set_xlim(-0.4, len(v) - 0.6)
    ax.set_xticks(x, [n for n, _, _ in v], fontsize=6.8)
    ax.set_ylabel("secondes (log)")
    ax.legend(loc="upper right")
    ax.set_title("Six versions du chemin rapide, mesurées (médianes)")
    sans_cadre(ax)
    grille(ax)
    sauver(fig, "optimisations")


def fig_chemin_lent():
    s = SLOW[(SLOW["ts"] >= RUN0) & (SLOW["ts"] <= RUN1)]
    etapes = {"pagerank_ms": "PageRank", "scc_ms": "SCC + élagage", "long_cycles_ms": "cycles longs",
              "mules_ms": "mules", "scatter_ms": "scatter-gather", "pivot_ms": "pivots",
              "clusters_ms": "Connected Comp.", "metrics_ms": "métriques comptes"}
    data, noms = [], []
    for k, n in etapes.items():
        if k in s:
            v = s[k].dropna() / 1000
            v = v[v > 0]
            if len(v):
                data.append(v)
                noms.append(n)
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.3))
    bp = ax.boxplot(data, vert=False, widths=0.55, showfliers=False, patch_artist=True,
                    medianprops={"color": ENCRE}, whiskerprops={"color": GRIS}, capprops={"color": GRIS})
    for b in bp["boxes"]:
        b.set(facecolor="#dceafb", edgecolor=BLEU)
    ax.set_yticks(range(1, len(noms) + 1), noms, fontsize=7.5)
    ax.set_xlabel("durée par passage (s)")
    tot = s["total_ms"] / 1000
    ax.set_title(f"Chemin lent : {len(s)} passages, durée médiane {fr(tot.median(), 0)} s pour une cadence de 90 s")
    sans_cadre(ax)
    grille(ax, "x")
    sauver(fig, "chemin_lent")


# =========================================================================== CH. 9 — résultats
def fig_rappel_precision():
    rec = pd.DataFrame(RES["recall"]).set_index("type").reindex(TYPES_SC).dropna()
    pre = pd.DataFrame(RES["precision"]).set_index("alert_type").reindex(TYPES_AL).dropna()
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(LARGEUR, 2.4), gridspec_kw={"wspace": 0.7})
    a1.barh([NOM_SC[t] for t in rec.index][::-1], rec["rappel"][::-1] * 100,
            color=[COUL_SC[t] for t in rec.index][::-1], height=0.6)
    for i, (t, r) in enumerate(rec[::-1].iterrows()):
        a1.text(min(r["rappel"] * 100 + 2, 104), i, f"{int(r['detectes'])}/{int(r['injectes'])}", va="center", fontsize=7.5)
    a1.set_xlim(0, 118)
    a1.set_title(f"Rappel par fraude injectée : {fr(RES['overall_recall'] * 100, 1)} %")
    a1.set_xlabel("%")
    a2.barh([NOM_AL[t] for t in pre.index][::-1], pre["precision"][::-1] * 100,
            color=[COUL_AL[t] for t in pre.index][::-1], height=0.6)
    for i, (t, r) in enumerate(pre[::-1].iterrows()):
        a2.text(min(r["precision"] * 100 + 2, 104), i, f"{int(r['vraies'])}/{int(r['alertes'])}", va="center", fontsize=7.5)
    a2.set_xlim(0, 118)
    a2.set_title(f"Précision par type d'alerte : {fr(RES['overall_precision'] * 100, 1)} %")
    a2.set_xlabel("%")
    for ax in (a1, a2):
        ax.tick_params(axis="y", labelsize=7.5)
        sans_cadre(ax)
        grille(ax, "x")
    sauver(fig, "rappel_precision")


def fig_delais():
    e = pd.read_csv(D / "evaluation_scenarios.csv")
    e["injecte"] = pd.to_datetime(e["injecte"], utc=True, format="ISO8601")
    # mêmes fraudes que le calcul du rappel (injectées au moins 8 min avant l'évaluation)
    e = e[e["injecte"] < pd.Timestamp(RES["generated_at"]) - pd.Timedelta(seconds=490)]
    e = e[e["detecte"] & e["delai_s"].notna()]
    fig, ax = plt.subplots(figsize=(LARGEUR, 2.3))
    rng = np.random.default_rng(1)
    ax.axvline(90, color=GRIS, linewidth=0.8, linestyle=":")
    ax.text(92, -0.75, "cadence du chemin lent (90 s)", fontsize=7, color=ENCRE2)
    for i, t in enumerate(TYPES_SC):
        v = e[e["type"] == t]["delai_s"].clip(lower=0.5)
        ax.scatter(v, i + rng.uniform(-0.18, 0.18, len(v)), s=16, color=COUL_SC[t], alpha=0.85, zorder=3)
        if len(v):
            ax.plot([v.median()] * 2, [i - 0.3, i + 0.3], color=ENCRE, linewidth=1.6, zorder=4)
            ax.text(v.max() * 1.3, i, f"médiane {fr(v.median(), 1 if v.median() < 10 else 0)} s", va="center",
                    fontsize=7.5)
    ax.set_xscale("log")
    ax.set_xlim(0.8, 4000)
    ax.set_ylim(len(TYPES_SC) - 0.5, -1.1)
    ax.set_yticks(range(len(TYPES_SC)), [NOM_SC[t] for t in TYPES_SC], fontsize=7.5)
    ax.set_xlabel("délai entre le dernier virement de la fraude et l'alerte (s, log)")
    ax.set_title("Délai de détection de chaque fraude injectée")
    sans_cadre(ax)
    grille(ax, "x")
    sauver(fig, "delais_detection")


def fig_latence():
    a = AL_RUN[(AL_RUN["detector"] == "fast-path") & (AL_RUN["risk_level"] != "LOW")]["latency_ms"].dropna() / 1000
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(LARGEUR, 2.1), gridspec_kw={"wspace": 0.35})
    a1.hist(a, bins=np.arange(0, a.max() + 1, 0.75), color=BLEU, edgecolor="white")
    a1.set_xlabel("latence ingestion Kafka → alerte (s)")
    a1.set_ylabel("alertes")
    a1.set_title(f"{len(a)} alertes du chemin rapide")
    v = np.sort(a.to_numpy())
    a2.plot(v, np.arange(1, len(v) + 1) / len(v) * 100, color=BLEU, linewidth=1.8)
    for q, c in ((0.5, ENCRE), (0.95, ORANGE)):
        x = np.quantile(v, q)
        a2.axvline(x, color=c, linewidth=0.9, linestyle="--")
        a2.text(x + 0.2, q * 100 - 14, f"p{int(q * 100)} = {fr(x, 1)} s", fontsize=7.5, color=c)
    a2.set_xlabel("latence (s)")
    a2.set_ylabel("% des alertes")
    a2.set_title("Distribution cumulée")
    for ax in (a1, a2):
        sans_cadre(ax)
        grille(ax)
    sauver(fig, "latence_chemin_rapide")


def fig_ablation():
    from matplotlib.patches import Patch
    ab = json.load(open(D / "ablation.json", encoding="utf-8"))
    noms = ["motif seul", "+ ordre temporel\net durée ≤ 10 min", "+ score ≥ MEDIUM"]
    fig, ax = plt.subplots(figsize=(LARGEUR, 1.95))
    for i, a in enumerate(ab):
        ax.barh(i, a["cycles"], color=GRILLE, height=0.6)
        ax.barh(i, a["vrais"], color=BLEU, height=0.6)
        ax.text(a["cycles"] + 3, i, f"{a['cycles']} cycles · précision {fr(a['precision'] * 100, 1)} %",
                va="center", fontsize=8)
    ax.legend(handles=[Patch(color=BLEU, label=f"cycles d'une fraude injectée ({ab[0]['vrais']})"),
                       Patch(color=GRILLE, label="boucles fortuites du trafic légitime")],
              loc="lower right", fontsize=7)
    ax.set_yticks(range(3), noms, fontsize=7.5)
    ax.invert_yaxis()
    ax.set_xlim(0, 380)
    ax.set_xlabel("cycles retenus")
    ax.set_title(f"Ablation sur {fr(ab[0]['aretes_analysees'])} transactions : aucun carrousel perdu "
                 f"({ab[-1]['carrousels_retrouves']}/{ab[0]['carrousels_retrouves']})")
    sans_cadre(ax)
    grille(ax, "x")
    sauver(fig, "ablation")


def fig_benchmark():
    b = pd.DataFrame(json.load(open(D / "benchmark.json"))["results"])
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(LARGEUR, 2.2), gridspec_kw={"wspace": 0.35})
    a1.plot(b["rate_target"], b["rate_target"], color=GRIS, linestyle="--", linewidth=1, label="idéal")
    a1.plot(b["rate_target"], b["processed_tx_s"], color=BLEU, marker="o", linewidth=1.8, label="débit traité")
    a1.set_xlabel("débit injecté (tx/s)")
    a1.set_ylabel("débit traité (tx/s)")
    a1.legend()
    a1.set_title("Le débit suit la charge")
    a2.plot(b["rate_target"], b["latency_s_p50"], color=BLEU, marker="o", linewidth=1.8, label="p50")
    a2.plot(b["rate_target"], b["latency_s_p95"], color=ORANGE, marker="o", linewidth=1.8, label="p95")
    a2.set_ylim(0, 4)
    a2.set_xlabel("débit injecté (tx/s)")
    a2.set_ylabel("latence carrousel (s)")
    a2.legend()
    a2.set_title("La latence reste vers 2 s")
    for ax in (a1, a2):
        sans_cadre(ax)
        grille(ax)
    sauver(fig, "benchmark")


def fig_historique():
    runs = [("A", 100.0, 81.5, 10.5), ("B", 99.2, 100.0, 40.7), ("C", 100.0, 93.9, 76.9),
            ("D", 98.8, 90.7, 23.7), ("E (final)", 99.0, 92.9, 10.7)]
    x = np.arange(len(runs))
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(LARGEUR, 2.5), gridspec_kw={"wspace": 0.3})
    a1.plot(x, [p for _, p, _, _ in runs], color=BLEU, marker="o", linewidth=1.8, label="précision")
    a1.plot(x, [r for _, _, r, _ in runs], color=ORANGE, marker="o", linewidth=1.8, label="rappel")
    a1.set_ylim(75, 103)
    a1.set_ylabel("%")
    a1.legend(loc="lower right")
    a1.set_title("Qualité de détection")
    a2.bar(x, [lat for *_, lat in runs], color=[GRIS] * 4 + [BLEU], width=0.6)
    for i, (*_, lat) in enumerate(runs):
        a2.text(i, lat + 1.5, f"{fr(lat, 1)} s", ha="center", fontsize=7.5)
    a2.set_title("Latence p95 du chemin rapide")
    a2.set_ylabel("s")
    for ax in (a1, a2):
        ax.set_xticks(x, [n for n, *_ in runs], fontsize=7.5)
        ax.set_xlabel("run d'évaluation")
        sans_cadre(ax)
        grille(ax)
    sauver(fig, "historique_runs")


if __name__ == "__main__":
    for f in (fig_trafic, fig_activite, fig_montants, fig_scenarios, fig_qualite, fig_etapes, fig_fenetrage,
              fig_elagage, fig_score, fig_scores_types, fig_gros_flux, fig_pagerank, fig_mule, fig_reseau,
              fig_delta, fig_panne, fig_memoire, fig_optimisations, fig_chemin_lent, fig_rappel_precision,
              fig_delais, fig_latence, fig_ablation, fig_benchmark, fig_historique):
        try:
            f()
        except Exception as exc:  # une figure en échec ne bloque pas les autres
            print("ÉCHEC", f.__name__, type(exc).__name__, exc)
