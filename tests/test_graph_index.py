"""Tests de l'index d'adjacence du graphe glissant (pur Python)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fraud.graph_index import SlidingGraphIndex, triangle_candidate_ids

T0 = datetime(2026, 10, 7, 10, 0, tzinfo=timezone.utc)


def e(tid, src, dst, sec):
    return {"transaction_id": tid, "src": src, "dst": dst, "montant": 1.0,
            "event_time": T0 + timedelta(seconds=sec), "type_operation": "X", "kafka_timestamp": None}


def test_pruning_keeps_only_edges_that_can_close_a_triangle():
    new = [("u3", "Z", "X"), ("n1", "P", "Q")]
    incident = [("u1", "X", "Y"), ("u2", "Y", "Z"), ("x9", "X", "K"), ("x8", "L", "Z")]
    assert triangle_candidate_ids(new, incident) == {"u3", "n1", "u1", "u2"}


def test_index_candidates_and_dedup():
    idx = SlidingGraphIndex(timedelta(minutes=12))
    idx.add([e("t1", "A", "B", 0), e("t2", "B", "C", 10), e("noise", "B", "D", 12), e("x", "Q", "R", 13)])
    idx.add([e("t1", "A", "B", 0)])  # doublon ignoré
    assert len(idx) == 4 and "t1" in idx
    idx.add([e("t3", "C", "A", 20)])
    assert idx.candidates(["t3"]) == {"t1", "t2", "t3"}


def test_eviction_respects_retention():
    idx = SlidingGraphIndex(timedelta(minutes=12))
    idx.add([e("old", "A", "B", 0), e("mid", "B", "C", 400)])
    idx.add([e("new", "C", "D", 13 * 60)])
    assert idx.evict() == 1
    assert "old" not in idx and "mid" in idx and "A" not in idx.out


def test_velocity_spike_merchant_vs_mule_and_cold_start():
    from fraud.graph_index import velocity_spike
    t_out = T0 + timedelta(minutes=30)
    steady = [T0 + timedelta(seconds=30 * i) for i in range(58)]  # 2 dépôts/min depuis 30 min
    ok, base = velocity_spike(steady, t_out, n_deposits=12, history_start=T0)
    assert ok is False and base > 10                      # commerçant : pas de pic
    ok, base = velocity_spike([], t_out, n_deposits=10, history_start=T0)
    assert ok is True and base == 0                       # compte silencieux puis rafale : mule
    ok, base = velocity_spike([], t_out, n_deposits=10, history_start=t_out - timedelta(minutes=8))
    assert ok is None                                     # démarrage à froid : on ne juge pas


def test_deposit_log_eviction():
    from fraud.graph_index import DepositLog
    log = DepositLog(timedelta(minutes=40))
    log.add([e("a", "X", "M", 0), e("b", "Y", "M", 45 * 60)])
    log.evict()
    assert len(log.get("M")) == 1 and log.start == T0
