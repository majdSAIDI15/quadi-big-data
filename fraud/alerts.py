"""Construction des alertes et de leurs sous-graphes (pur Python).

L'identifiant d'alerte est déterministe (empreinte du type et des éléments
détectés) : si un micro-batch est rejoué après une panne, la même alerte
produit le même `alert_id`, ce qui rend les consommateurs idempotents.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from .scoring import Score

ALERT_TYPES = ["CYCLE", "CYCLE_LONG", "MULE", "SCATTER_GATHER", "PIVOT", "CLUSTER"]


def alert_id(alert_type: str, *keys: str) -> str:
    digest = hashlib.sha1("|".join([alert_type, *sorted(keys)]).encode()).hexdigest()[:16].upper()
    return f"ALT-{digest}"


@dataclass
class Alert:
    alert_id: str
    alert_type: str
    risk_score: int
    risk_level: str
    accounts: list[str]
    transaction_ids: list[str]
    total_amount: float
    window_start: datetime
    window_end: datetime
    detected_at: datetime
    explanation: str
    features: dict
    devise: str = "MAD"
    cycle_length: int | None = None
    latency_ms: int | None = None
    edges: list[dict] = field(default_factory=list)      # sous-graphe : transactions
    roles: dict[str, str] = field(default_factory=dict)  # compte -> rôle

    @property
    def dt(self) -> str:
        return self.detected_at.astimezone(timezone.utc).strftime("%Y-%m-%d")

    def row(self) -> dict:
        d = asdict(self)
        d.pop("edges")
        d.pop("roles")
        d["features"] = json.dumps(self.features, ensure_ascii=False, default=str)
        d["dt"] = self.dt
        return d

    def kafka_value(self) -> str:
        d = self.row()
        for k in ("window_start", "window_end", "detected_at"):
            d[k] = d[k].astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        d["features"] = self.features
        return json.dumps(d, ensure_ascii=False, default=str)

    def vertex_rows(self, pagerank: dict[str, float] | None = None) -> list[dict]:
        pagerank = pagerank or {}
        return [{"alert_id": self.alert_id, "account_id": a, "role": self.roles.get(a, "MEMBRE"),
                 "pagerank": pagerank.get(a), "risk_level": self.risk_level, "dt": self.dt}
                for a in self.accounts]

    def edge_rows(self) -> list[dict]:
        return [{"alert_id": self.alert_id, "transaction_id": e["transaction_id"], "src": e["src"],
                 "dst": e["dst"], "montant": float(e["montant"]), "event_time": e["event_time"],
                 "type_operation": e.get("type_operation"), "risk_level": self.risk_level, "dt": self.dt}
                for e in self.edges]


def make_alert(alert_type: str, key: list[str], score: Score, accounts: list[str], edges: list[dict],
               explanation: str, detected_at: datetime, roles: dict[str, str] | None = None,
               cycle_length: int | None = None, latency_ms: int | None = None) -> Alert:
    times = [e["event_time"] for e in edges] or [detected_at]
    return Alert(
        alert_id=alert_id(alert_type, *key),
        alert_type=alert_type,
        risk_score=score.score,
        risk_level=score.level or "LOW",
        accounts=list(dict.fromkeys(accounts)),
        transaction_ids=[e["transaction_id"] for e in edges],
        total_amount=round(sum(float(e["montant"]) for e in edges), 2),
        window_start=min(times),
        window_end=max(times),
        detected_at=detected_at,
        explanation=explanation,
        features=score.features,
        cycle_length=cycle_length,
        latency_ms=latency_ms,
        edges=edges,
        roles=roles or {},
    )
