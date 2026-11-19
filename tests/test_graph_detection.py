"""Tests Spark : motifs GraphFrames, composantes, typologies et schéma Delta.

Exécution (dans le conteneur) : docker compose run --rm spark-streaming pytest -q
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

T0 = datetime(2026, 10, 7, 10, 0, 0)
SCHEMA = ("transaction_id STRING, src STRING, dst STRING, montant DOUBLE, event_time TIMESTAMP, "
          "type_operation STRING, kafka_timestamp TIMESTAMP")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ["CHECKPOINT_DIR"] = str(tmp_path_factory.mktemp("ckpt"))
    from fraud.spark_session import build_spark
    s = build_spark("tests", master="local[2]")
    yield s
    s.stop()


def E(spark, rows):
    """rows : (id, src, dst, montant, secondes depuis T0)"""
    data = [(i, s, d, float(m), T0 + timedelta(seconds=t), "VIREMENT_COMMERCIAL", T0 + timedelta(seconds=t))
            for i, s, d, m, t in rows]
    return spark.createDataFrame(data, SCHEMA)


def test_triangle_temporal_unique_rotation(spark):
    from fraud.detection import find_cycles
    edges = E(spark, [("t1", "A", "B", 50_000, 0), ("t2", "B", "C", 49_000, 60), ("t3", "C", "A", 48_000, 120),
                      ("x1", "A", "D", 10, 5), ("x2", "D", "A", 10, 6)])  # aller-retour : pas un triangle
    rows = find_cycles(edges, 3, 600, temporal=True).collect()
    assert len(rows) == 1 and rows[0].e1_transaction_id == "t1"


def test_triangle_out_of_order_rejected_but_found_without_temporal_filter(spark):
    from fraud.detection import find_cycles
    # Aucune rotation n'est chronologique : A->B (0 s), B->C (100 s), C->A (50 s)
    edges = E(spark, [("t1", "A", "B", 1, 0), ("t2", "B", "C", 1, 100), ("t3", "C", "A", 1, 50)])
    assert find_cycles(edges, 3, 600, temporal=True).count() == 0
    assert find_cycles(edges, 3, 600, temporal=False).count() == 1  # une seule rotation (ablation)


def test_cycle_longer_than_window_rejected(spark):
    from fraud.detection import find_cycles
    edges = E(spark, [("t1", "A", "B", 1, 0), ("t2", "B", "C", 1, 300), ("t3", "C", "A", 1, 700)])
    assert find_cycles(edges, 3, 600).count() == 0


def test_incremental_search_only_reports_cycles_closed_by_new_edges(spark):
    from pyspark.sql import functions as F

    from fraud.detection import candidate_edges, find_cycles
    window = E(spark, [("t1", "A", "B", 1, 0), ("t2", "B", "C", 1, 10), ("t3", "C", "A", 1, 20),  # ancien
                       ("u1", "X", "Y", 1, 30), ("u2", "Y", "Z", 1, 40), ("u3", "Z", "X", 1, 50),  # nouveau
                       ("n1", "P", "Q", 1, 55)])
    new_ids = window.filter(F.col("transaction_id").isin("u3", "n1")).select("transaction_id")
    cand = candidate_edges(window, new_ids)
    assert {r.transaction_id for r in cand.collect()} == {"u1", "u2", "u3", "n1"}
    rows = find_cycles(cand, 3, 600, only_new=True).collect()
    assert [r.e3_transaction_id for r in rows] == ["u3"]


def test_long_cycle_found_inside_scc(spark):
    from fraud.detection import long_cycles_in_sccs, strongly_connected
    hv = E(spark, [("a", "A", "B", 1e5, 0), ("b", "B", "C", 1e5, 30), ("c", "C", "D", 1e5, 60),
                   ("d", "D", "A", 1e5, 90), ("z", "D", "Z", 1e5, 95)])
    scc = strongly_connected(hv)
    sizes = {r.id: r.scc_size for r in scc.collect()}
    assert sizes["A"] == 4
    assert "Z" not in sizes  # Z n'a pas d'arête sortante : élagué avant la SCC (singleton)
    found = long_cycles_in_sccs(hv, scc, (4, 5), 600)
    assert len(found[4]) == 1 and len(found.get(5, [])) == 0


def test_pagerank_and_components(spark):
    from fraud.detection import connected_components, pagerank
    edges = E(spark, [("1", "A", "H", 1, 0), ("2", "B", "H", 1, 0), ("3", "C", "H", 1, 0), ("4", "X", "Y", 1, 0)])
    pr = {r.id: r.pagerank for r in pagerank(edges).collect()}
    assert max(pr, key=pr.get) == "H"
    cc = {r.id: r.cluster_id for r in connected_components(edges).collect()}
    assert cc["A"] == cc["H"] != cc["X"]


def test_mule_detection(spark):
    from fraud.detection import find_mules
    rows = [(f"in{i}", f"S{i}", "M", 7_000, i * 10) for i in range(8)]
    rows += [("out", "M", "BEN", 53_000, 120), ("m1", "SHOP", "M2", 100, 0)]
    # Compte actif légitime : reçoit de 8 émetteurs mais paie AVANT et plus qu'il ne reçoit
    rows += [(f"a{i}", f"C{i}", "ACTIF", 7_000, 60 + i) for i in range(8)]
    rows += [("big", "ACTIF", "FOURN", 90_000, 10)]
    # Un paiement légitime reçu APRÈS le retrait ne doit pas masquer la mule
    rows += [("late_legit", "VOISIN", "M", 4_000, 200)]
    found = find_mules(E(spark, rows), 6, 30_000, 0.8).collect()
    assert [r.account for r in found] == ["M"]
    assert found[0].n_senders == 8 and found[0].inflow == 56_000


def test_scatter_gather_detection(spark):
    from fraud.detection import find_scatter_gather
    rows = []
    for i in range(4):
        rows += [(f"o{i}", "SRC", f"M{i}", 25_000, i), (f"i{i}", f"M{i}", "SINK", 24_500, 60 + i)]
    found = find_scatter_gather(E(spark, rows), 3, 600).collect()
    assert len(found) == 1 and found[0].k == 4 and found[0].sink == "SINK"


def test_delta_schema_and_constraints(spark, tmp_path):
    from fraud_graph_delta_schema import DEFS, ensure_schema
    lake = str(tmp_path / "lake")
    ensure_schema(spark, lake, verbose=False)
    ensure_schema(spark, lake, verbose=False)  # idempotent
    path = f"{lake}/bronze/transactions"
    good = [("T1", "A", "B", 10.0, "MAD", "X", T0, 0, 1, T0, T0, "2026-10-07")]
    spark.createDataFrame(good, DEFS["transactions"].spark_schema()).write.format("delta").mode("append").save(path)
    bad = [("T2", "A", "B", -5.0, "MAD", "X", T0, 0, 2, T0, T0, "2026-10-07")]
    with pytest.raises(Exception, match="(?i)constraint|check"):
        spark.createDataFrame(bad, DEFS["transactions"].spark_schema()).write.format("delta").mode("append").save(path)
    props = spark.sql(f"DESCRIBE DETAIL delta.`{lake}/gold/fraud_alerts`").first()
    assert props.partitionColumns == ["risk_level", "dt"]
    assert props.properties.get("delta.appendOnly") == "true"



def test_mule_velocity_spike_excludes_steady_merchant(spark):
    from fraud.detection import find_mules
    # Commerçant : reçoit ~2 paiements/min depuis 30 min, puis un gros paiement fournisseur
    rows = [(f"h{i}", f"C{i}", "SHOP", 4_000, i * 30) for i in range(60)]
    rows += [("pay", "SHOP", "FOURN", 0.9 * 4_000 * 12, 30 * 60 + 5)]
    # Mule : compte silencieux, rafale de 10 dépôts puis retrait
    rows += [(f"m{i}", f"S{i}", "MULE", 6_000, 30 * 60 - 300 + i * 20) for i in range(10)]
    rows += [("cash", "MULE", "BEN", 57_000, 30 * 60 + 30)]
    edges = E(spark, rows)
    found = find_mules(edges, 6, 30_000, 0.75, history=edges).collect()
    assert [r.account for r in found] == ["MULE"]
