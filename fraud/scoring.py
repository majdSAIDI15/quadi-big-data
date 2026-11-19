"""Score de risque (0-100), niveau et explication en langage clair.

Module pur Python : chaque règle est lisible et testable, et chaque point de
score est justifié par une phrase (exigence d'explicabilité en LCB-FT).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from statistics import median

# Seuils des niveaux (score minimal inclus). En dessous de LOW : non significatif.
LEVEL_THRESHOLDS = [("CRITICAL", 90), ("HIGH", 70), ("MEDIUM", 40), ("LOW", 20)]
LEVEL_RANK = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}


def risk_level(score: float) -> str | None:
    for level, threshold in LEVEL_THRESHOLDS:
        if score >= threshold:
            return level
    return None


def level_at_least(level: str | None, minimum: str) -> bool:
    return level is not None and LEVEL_RANK[level] >= LEVEL_RANK[minimum]


def fmt_mad(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ") + " MAD"


def fmt_duration(seconds: float) -> str:
    seconds = int(round(seconds))
    m, s = divmod(seconds, 60)
    return f"{m} min {s:02d} s" if m else f"{s} s"


@dataclass
class Score:
    score: int
    reasons: list[str] = field(default_factory=list)
    features: dict = field(default_factory=dict)

    @property
    def level(self) -> str | None:
        return risk_level(self.score)


class _Builder:
    def __init__(self, base: int = 0):
        self.total = base
        self.reasons: list[str] = []

    def add(self, points: int, reason: str) -> None:
        if points:
            self.total += points
            self.reasons.append(f"{reason} (+{points})")

    def done(self, features: dict) -> Score:
        return Score(min(100, max(0, int(round(self.total)))), self.reasons, features)


def pagerank_points(pct: float | None) -> int:
    if pct is None:
        return 0
    return 10 if pct >= 0.99 else 5 if pct >= 0.95 else 0


# --------------------------------------------------------------------------- cycles
def score_cycle(edges: list[dict], pagerank_pct: dict[str, float] | None = None) -> Score:
    """edges : transactions du cycle triées dans l'ordre temporel, chacune avec
    src, dst, montant, event_time (datetime) et type_operation."""
    pagerank_pct = pagerank_pct or {}
    amounts = [float(e["montant"]) for e in edges]
    times: list[datetime] = [e["event_time"] for e in edges]
    n = len(edges)
    ratio = max(amounts) / max(min(amounts), 0.01)
    hop_losses = [1 - amounts[i + 1] / amounts[i] for i in range(n - 1)]
    skimming = all(0.002 <= loss <= 0.06 for loss in hop_losses)
    span = (max(times) - min(times)).total_seconds()
    retention = amounts[-1] / amounts[0]
    accounts = [e["src"] for e in edges]
    max_pct = max((pagerank_pct.get(a, 0.0) for a in accounts), default=0.0)

    b = _Builder()
    if ratio <= 1.10:
        b.add(30, f"montants quasi identiques à chaque étape (écart max {100 * (ratio - 1):.1f} %)")
    elif ratio <= 1.25:
        b.add(15, f"montants proches à chaque étape (écart max {100 * (ratio - 1):.0f} %)")
    if skimming:
        avg_loss = 100 * sum(hop_losses) / len(hop_losses)
        b.add(15, f"perte régulière de {avg_loss:.1f} % par étape (commission typique d'un carrousel)")
    if span <= 300:
        b.add(20, f"boucle bouclée en {fmt_duration(span)}")
    elif span <= 600:
        b.add(10, f"boucle bouclée en {fmt_duration(span)}")
    if min(amounts) >= 100_000:
        b.add(15, f"montants élevés (≥ {fmt_mad(100_000)})")
    elif min(amounts) >= 30_000:
        b.add(10, f"montants significatifs (≥ {fmt_mad(30_000)})")
    elif min(amounts) >= 10_000:
        b.add(5, "montants supérieurs à 10 000 MAD")
    if len({e.get("type_operation") for e in edges}) == 1:
        b.add(5, f"même type d'opération sur toute la boucle ({edges[0].get('type_operation')})")
    b.add(pagerank_points(max_pct), "un compte du cycle est un pivot du réseau (PageRank élevé)")
    if n >= 4:
        b.add(5, f"cycle long ({n} sauts) : montage plus élaboré")

    return b.done({
        "cycle_length": n, "amount_ratio": round(ratio, 4), "retention": round(retention, 4),
        "span_seconds": round(span, 1), "skimming": skimming, "max_pagerank_pct": round(max_pct, 4),
    })


def explain_cycle(edges: list[dict], score: Score) -> str:
    path = " → ".join([e["src"] for e in edges] + [edges[0]["src"]])
    amounts = [float(e["montant"]) for e in edges]
    head = (f"{path} en {fmt_duration(score.features['span_seconds'])}, "
            f"{len(edges)} virements de {fmt_mad(min(amounts))} à {fmt_mad(max(amounts))}, "
            f"{100 * score.features['retention']:.1f} % du montant revient à l'origine.")
    return head + " Indices : " + "; ".join(score.reasons) + "." if score.reasons else head


# --------------------------------------------------------------------------- mules
def score_mule(n_senders: int, inflow: float, outflow: float, incoming: list[float],
               delay_seconds: float | None, burst_seconds: float | None = None,
               baseline_per_burst: float | None = None) -> Score:
    """outflow : montant du virement de retrait (le plus gros virement sortant)."""
    flow_through = outflow / inflow if inflow else 0.0
    med_in = median(incoming) if incoming else 0.0
    b = _Builder(base=35)
    b.reasons.append(f"{n_senders} émetteurs distincts vers un même compte (+35)")
    if flow_through >= 0.90:
        b.add(20, f"un seul virement reprend {100 * flow_through:.0f} % des dépôts (vidage du compte)")
    elif flow_through >= 0.80:
        b.add(10, f"un seul virement reprend {100 * flow_through:.0f} % des dépôts")
    elif flow_through >= 0.70:
        b.add(5, f"un seul virement reprend {100 * flow_through:.0f} % des dépôts")
    if n_senders >= 10:
        b.add(15, "dépôts très fractionnés (≥ 10 émetteurs)")
    elif n_senders >= 6:
        b.add(5, "dépôts fractionnés (≥ 6 émetteurs)")
    if baseline_per_burst is not None:
        factor = len(incoming) / max(baseline_per_burst, 1.0)
        b.add(10, f"{len(incoming)} dépôts en rafale contre {baseline_per_burst:.1f} habituellement "
                  f"(pic de vélocité x{factor:.0f})")
    elif burst_seconds is not None and len(incoming) / max(burst_seconds / 60, 1.0) >= 2:
        b.add(10, f"{len(incoming)} dépôts en {fmt_duration(burst_seconds)} (rafale)")
    if med_in and med_in < 10_000:
        b.add(5, f"dépôts médians de {fmt_mad(med_in)}, sous le seuil de 10 000 MAD")
    if delay_seconds is not None and 0 < delay_seconds <= 300:
        b.add(10, f"retrait {fmt_duration(delay_seconds)} après le dernier dépôt")
    return b.done({"n_senders": n_senders, "inflow": round(inflow, 2), "outflow": round(outflow, 2),
                   "flow_through": round(flow_through, 4), "median_in": round(med_in, 2),
                   "delay_seconds": delay_seconds, "burst_seconds": burst_seconds,
                   "baseline_per_burst": baseline_per_burst})


def explain_mule(account: str, score: Score) -> str:
    f = score.features
    return (f"Compte mule probable {account} : {f['n_senders']} dépôts pour {fmt_mad(f['inflow'])}, "
            f"puis {fmt_mad(f['outflow'])} ressortis ({100 * f['flow_through']:.0f} %). "
            "Indices : " + "; ".join(score.reasons) + ".")


# --------------------------------------------------------------------------- scatter-gather
def score_scatter_gather(n_intermediaries: int, out_parts: list[float], in_parts: list[float]) -> Score:
    total_out, total_in = sum(out_parts), sum(in_parts)
    ratio = max(out_parts) / max(min(out_parts), 0.01)
    retention = total_in / total_out if total_out else 0.0
    b = _Builder(base=40)
    b.reasons.append(f"{n_intermediaries} intermédiaires entre une même source et une même destination (+40)")
    if n_intermediaries >= 5:
        b.add(15, "éclatement en ≥ 5 virements")
    elif n_intermediaries >= 3:
        b.add(10, "éclatement en ≥ 3 virements")
    if ratio <= 1.10:
        b.add(15, "parts de taille quasi identique (découpage artificiel)")
    if retention >= 0.95:
        b.add(15, f"{100 * retention:.1f} % du montant reconstitué à destination")
    if total_out >= 100_000:
        b.add(10, f"montant total élevé ({fmt_mad(total_out)})")
    return b.done({"n_intermediaries": n_intermediaries, "total_out": round(total_out, 2),
                   "total_in": round(total_in, 2), "part_ratio": round(ratio, 4),
                   "retention": round(retention, 4)})


def explain_scatter_gather(source: str, sink: str, score: Score) -> str:
    f = score.features
    return (f"{source} éclate {fmt_mad(f['total_out'])} via {f['n_intermediaries']} intermédiaires, "
            f"reconstitués chez {sink} ({fmt_mad(f['total_in'])}). Indices : " + "; ".join(score.reasons) + ".")


# --------------------------------------------------------------------------- pivots & clusters
def score_pivot(n_cycles: int, pagerank_pct: float | None) -> Score:
    b = _Builder(base=45)
    b.reasons.append(f"impliqué dans {n_cycles} cycles suspects (+45)")
    b.add(min(30, 15 * (n_cycles - 1)), "participation à plusieurs boucles de blanchiment")
    pts = 15 if (pagerank_pct or 0) >= 0.99 else 8 if (pagerank_pct or 0) >= 0.95 else 0
    b.add(pts, f"PageRank dans le top {max(1, round(100 * (1 - (pagerank_pct or 0))))} % du graphe des gros flux")
    return b.done({"n_cycles": n_cycles, "pagerank_pct": round(pagerank_pct or 0.0, 4)})


def explain_pivot(account: str, score: Score) -> str:
    return f"Compte pivot {account}. Indices : " + "; ".join(score.reasons) + "."


def score_cluster(n_accounts: int, n_alerts: int, total_amount: float) -> Score:
    b = _Builder(base=45)
    b.reasons.append(f"composante connexe reliant {n_alerts} alertes (+45)")
    b.add(min(30, 10 * (n_alerts - 1)), "plusieurs schémas frauduleux interconnectés")
    if n_accounts >= 6:
        b.add(10, f"réseau de {n_accounts} comptes")
    if total_amount >= 300_000:
        b.add(10, f"volume total de {fmt_mad(total_amount)}")
    return b.done({"n_accounts": n_accounts, "n_alerts": n_alerts, "total_amount": round(total_amount, 2)})


def explain_cluster(accounts: list[str], score: Score) -> str:
    shown = ", ".join(sorted(accounts)[:8]) + (" …" if len(accounts) > 8 else "")
    return f"Réseau de {len(accounts)} comptes ({shown}). Indices : " + "; ".join(score.reasons) + "."
