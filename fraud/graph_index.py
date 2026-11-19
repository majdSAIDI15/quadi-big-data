"""Index d'adjacence du graphe glissant (état du chemin rapide), pur Python.

Le chemin rapide garde en mémoire les arêtes des 12 dernières minutes (fenêtre de
10 min + watermark de 2 min) avec deux index : arêtes sortantes et entrantes par compte.
Pour chaque micro-batch, il en extrait en O(taille du lot) le petit sous-graphe où un
triangle peut se fermer ; le motif GraphFrames ne tourne que sur ce sous-graphe.

Ordre de grandeur : 100 tx/s x 12 min = 72 000 arêtes, soit ~15 Mo. Sur un cluster, cet
état serait partitionné par compte (state store Structured Streaming).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta


def triangle_candidate_ids(new_edges: list[tuple[str, str, str]],
                           incident: list[tuple[str, str, str]]) -> set[str]:
    """Arêtes pouvant former un triangle avec au moins une arête nouvelle.

    Un triangle contenant l'arête nouvelle u->v est fermé par v->w et w->u :
    v->w part d'un bénéficiaire d'arête nouvelle, w->u arrive sur un émetteur
    d'arête nouvelle, et w est commun aux deux ensembles.

    new_edges : (id, src, dst) du micro-batch ;
    incident  : (id, src, dst) des arêtes de la fenêtre partant d'un bénéficiaire
                ou arrivant sur un émetteur d'arête nouvelle.
    """
    heads = {d for _, _, d in new_edges}
    tails = {s for _, s, _ in new_edges}
    out_v = [(i, d) for i, s, d in incident if s in heads]
    in_u = [(i, s) for i, s, d in incident if d in tails]
    w = {d for _, d in out_v} & {s for _, s in in_u}
    return {i for i, _, _ in new_edges} | {i for i, d in out_v if d in w} | {i for i, s in in_u if s in w}


def velocity_spike(deposit_times, t_out: datetime, n_deposits: int, history_start: datetime,
                   burst_minutes: int = 6, history_minutes: int = 20,
                   ratio: float = 3.0) -> tuple[bool | None, float | None]:
    """Pic de vélocité d'un compte avant un retrait.

    Compare les `n_deposits` dépôts des `burst_minutes` minutes avant `t_out` au rythme
    habituel du compte sur les `history_minutes` minutes d'avant. Renvoie (pic ?, dépôts
    habituels par période de rafale), ou (None, None) si l'historique disponible est trop
    court (démarrage à froid : on ne juge pas)."""
    hi = t_out - timedelta(minutes=burst_minutes)
    lo = max(hi - timedelta(minutes=history_minutes), history_start)
    available = (hi - lo).total_seconds() / 60
    if available < burst_minutes:
        return None, None
    count = sum(1 for t in deposit_times if lo <= t < hi)
    per_burst = count * burst_minutes / available
    return n_deposits >= ratio * max(per_burst, 1.0), per_burst


class DepositLog:
    """Horodatages des dépôts reçus par compte (40 dernières minutes) : le rythme habituel
    de chaque compte, consulté par le chemin lent pour détecter les pics de vélocité."""

    def __init__(self, retention: timedelta = timedelta(minutes=40)):
        self.retention = retention
        self.times: dict[str, list[datetime]] = defaultdict(list)
        self.start: datetime | None = None
        self.max_time: datetime | None = None

    def add(self, edges: list[dict]) -> None:
        for e in edges:
            t = e["event_time"]
            self.times[e["dst"]].append(t)
            if self.start is None or t < self.start:
                self.start = t
            if self.max_time is None or t > self.max_time:
                self.max_time = t

    def evict(self) -> None:
        if self.max_time is None:
            return
        cutoff = self.max_time - self.retention
        for acc in list(self.times):
            kept = [t for t in self.times[acc] if t >= cutoff]
            if kept:
                self.times[acc] = kept
            else:
                del self.times[acc]

    def get(self, account: str) -> list[datetime]:
        return self.times.get(account, [])


class SlidingGraphIndex:
    def __init__(self, retention: timedelta):
        self.retention = retention
        self.edges: dict[str, dict] = {}
        self.out: dict[str, set[str]] = defaultdict(set)
        self.inc: dict[str, set[str]] = defaultdict(set)
        self.max_time: datetime | None = None

    def __len__(self) -> int:
        return len(self.edges)

    def __contains__(self, tx_id: str) -> bool:
        return tx_id in self.edges

    def add(self, edges: list[dict]) -> None:
        for e in edges:
            tid = e["transaction_id"]
            if tid in self.edges:
                continue
            self.edges[tid] = e
            self.out[e["src"]].add(tid)
            self.inc[e["dst"]].add(tid)
            if self.max_time is None or e["event_time"] > self.max_time:
                self.max_time = e["event_time"]

    def evict(self) -> int:
        """Retire les arêtes sorties de la fenêtre (+ marge du watermark)."""
        if self.max_time is None:
            return 0
        cutoff = self.max_time - self.retention
        old = [tid for tid, e in self.edges.items() if e["event_time"] < cutoff]
        for tid in old:
            e = self.edges.pop(tid)
            for idx, key in ((self.out, e["src"]), (self.inc, e["dst"])):
                s = idx.get(key)
                if s is not None:
                    s.discard(tid)
                    if not s:
                        del idx[key]
        return len(old)

    def candidates(self, new_ids: list[str]) -> set[str]:
        new = [(i, self.edges[i]["src"], self.edges[i]["dst"]) for i in new_ids if i in self.edges]
        heads = {d for _, _, d in new}
        tails = {s for _, s, _ in new}
        incident_ids = set()
        for v in heads:
            incident_ids |= self.out.get(v, set())
        for u in tails:
            incident_ids |= self.inc.get(u, set())
        incident = [(i, self.edges[i]["src"], self.edges[i]["dst"]) for i in incident_ids]
        return triangle_candidate_ids(new, incident)

    def rows(self, ids: set[str]) -> list[dict]:
        return [self.edges[i] for i in ids if i in self.edges]
