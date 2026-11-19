"""Tests pur Python : scénarios, scoring, alertes (aucune dépendance Spark/Kafka)."""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

import pytest

from fraud.alerts import alert_id, make_alert
from fraud.scenarios import (
    EXPECTED_ALERTS,
    SCENARIO_TYPES,
    AccountPopulation,
    build_cycle,
    build_scenario,
    malformed_transaction,
)
from fraud.scoring import (
    level_at_least,
    risk_level,
    score_cycle,
    score_mule,
    score_pivot,
    score_scatter_gather,
)

T0 = datetime(2026, 10, 7, 10, 15, tzinfo=timezone.utc)
FIELDS = {"transaction_id", "source_account", "target_account", "montant", "devise", "type_operation", "timestamp"}


@pytest.fixture(scope="module")
def pop():
    return AccountPopulation(2_000, seed=7)


def edges_from(scenario, start=T0):
    return [{"transaction_id": t["transaction_id"], "src": t["source_account"], "dst": t["target_account"],
             "montant": t["montant"], "event_time": start + timedelta(seconds=o),
             "type_operation": t["type_operation"]} for o, t in scenario.steps]


def test_payload_respects_subject_schema(pop):
    t = pop.normal_transaction()
    assert set(t) == FIELDS
    assert t["devise"] == "MAD" and t["montant"] > 0
    assert t["source_account"] != t["target_account"]
    assert t["transaction_id"].startswith("TXN-") and t["timestamp"].endswith("Z")


@pytest.mark.parametrize("kind", SCENARIO_TYPES)
def test_every_scenario_builds(pop, kind):
    s = build_scenario(kind, pop, tempo=1.0)
    gt = s.ground_truth()
    assert gt["expected_alert_types"] == EXPECTED_ALERTS[kind]
    assert len(set(s.transaction_ids)) == len(s.steps) > 0
    assert all(set(t) == FIELDS for _, t in s.steps)


def test_carousel_closes_the_loop_and_skims(pop):
    s = build_cycle(pop, 3)
    txs = [t for _, t in sorted(s.steps, key=lambda x: x[0])]
    assert txs[-1]["target_account"] == txs[0]["source_account"]
    assert all(txs[i + 1]["montant"] < txs[i]["montant"] for i in range(2))


def test_injected_carousel_scores_high_and_random_triangle_low(pop):
    sc = score_cycle(edges_from(build_cycle(pop, 3)))
    assert sc.level in ("HIGH", "CRITICAL"), sc
    rng = random.Random(1)
    noise = [{"transaction_id": f"T{i}", "src": a, "dst": b, "montant": rng.lognormvariate(8, 1),
              "event_time": T0 + timedelta(seconds=200 * i), "type_operation": op}
             for i, (a, b, op) in enumerate([("A", "B", "VIREMENT_PARTICULIER"), ("B", "C", "REGLEMENT_FACTURE"),
                                             ("C", "A", "TRANSFERT_INTERNE")])]
    assert not level_at_least(score_cycle(noise).level, "MEDIUM")


def test_long_cycle_bonus(pop):
    sc = score_cycle(edges_from(build_cycle(pop, 5)))
    assert sc.features["cycle_length"] == 5 and level_at_least(sc.level, "MEDIUM")


def test_mule_and_scatter_scores():
    mule = score_mule(10, 70_000, 66_000, [7_000] * 10, 60, burst_seconds=180)
    assert mule.level == "CRITICAL"
    # compte actif légitime : 6 émetteurs étalés, retrait partiel, pas d'ordre chronologique
    assert not level_at_least(score_mule(6, 40_000, 33_000, [6_000] * 6, 0, burst_seconds=540).level, "HIGH")
    sg = score_scatter_gather(5, [20_000] * 5, [19_700] * 5)
    assert level_at_least(sg.level, "HIGH")
    assert score_pivot(3, 0.995).level == "CRITICAL"


def test_risk_level_thresholds():
    assert [risk_level(x) for x in (0, 19, 20, 40, 70, 90, 100)] == \
        [None, None, "LOW", "MEDIUM", "HIGH", "CRITICAL", "CRITICAL"]


def test_alert_id_is_deterministic_and_order_free():
    assert alert_id("CYCLE", "t1", "t2", "t3") == alert_id("CYCLE", "t3", "t1", "t2")
    assert alert_id("CYCLE", "t1") != alert_id("MULE", "t1")


def test_alert_rows(pop):
    edges = edges_from(build_cycle(pop, 3))
    sc = score_cycle(edges)
    a = make_alert("CYCLE", [e["transaction_id"] for e in edges], sc, [e["src"] for e in edges], edges,
                   "x", T0, cycle_length=3)
    row = a.row()
    assert row["dt"] == "2026-10-07" and row["risk_level"] == sc.level and len(a.edge_rows()) == 3
    assert '"alert_type": "CYCLE"' in a.kafka_value()


def test_malformed_transactions_are_invalid(pop):
    for _ in range(30):
        t = malformed_transaction(pop)
        assert (t.get("montant", 1) <= 0 or "target_account" not in t
                or t["source_account"] == t.get("target_account"))
