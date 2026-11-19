"""Algorithmes de graphe (GraphFrames) appliqués aux transactions.

Toutes les fonctions prennent et rendent des DataFrames Spark « statiques » :
GraphFrames ne s'applique pas directement à un flux, le job de streaming les
appelle dans `foreachBatch`. Le script d'évaluation les réutilise hors ligne.

Schéma des arêtes attendu : transaction_id, src, dst, montant, event_time,
type_operation, kafka_timestamp (+ is_new pour la recherche incrémentale).
"""
from __future__ import annotations

from graphframes import GraphFrame
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

EDGE_FIELDS = ["transaction_id", "src", "dst", "montant", "event_time", "type_operation", "kafka_timestamp"]


def release(df: DataFrame | None) -> None:
    """Libère les blocs d'un DataFrame matérialisé par localCheckpoint.

    `DataFrame.unpersist()` n'a pas d'effet sur un point de contrôle local : ses blocs ne
    partent qu'au GC périodique de Spark (30 min par défaut). Dans un job de streaming qui
    en crée à chaque micro-batch, c'est une fuite mémoire : on libère donc explicitement
    les RDD sources du plan."""
    if df is None:
        return
    try:
        leaves = df._jdf.queryExecution().analyzed().collectLeaves()
        it = leaves.iterator()
        while it.hasNext():
            leaf = it.next()
            if leaf.getClass().getSimpleName() == "LogicalRDD":
                leaf.rdd().unpersist(False)
    except Exception:  # libération « au mieux » : ne doit jamais faire échouer un lot
        pass


def to_edges(tx: DataFrame) -> DataFrame:
    """Transactions (bronze) -> arêtes orientées compte émetteur -> compte bénéficiaire."""
    return tx.select(
        "transaction_id",
        F.col("source_account").alias("src"),
        F.col("target_account").alias("dst"),
        "montant", "event_time", "type_operation", "kafka_timestamp",
    )


def vertices_of(edges: DataFrame) -> DataFrame:
    return edges.select(F.col("src").alias("id")).union(edges.select(F.col("dst").alias("id"))).distinct()


def graph(edges: DataFrame) -> GraphFrame:
    return GraphFrame(vertices_of(edges), edges)


def _seconds(col: str) -> F.Column:
    return F.col(col).cast("double")


def _flatten(df: DataFrame, n: int) -> DataFrame:
    cols = []
    for i in range(1, n + 1):
        for f in EDGE_FIELDS:
            cols.append(F.col(f"e{i}.{f}").alias(f"e{i}_{f}"))
    return df.select(*cols)


# --------------------------------------------------------------------------- cycles
def motif_cycle(n: int) -> str:
    """'(v1)-[e1]->(v2); (v2)-[e2]->(v3); ...; (vn)-[en]->(v1)'"""
    return "; ".join(f"(v{i})-[e{i}]->(v{i % n + 1})" for i in range(1, n + 1))


def find_cycles(edges: DataFrame, n: int = 3, window_seconds: int = 600,
                temporal: bool = True, only_new: bool = False) -> DataFrame:
    """Cycles fermés de longueur n par motif finding GraphFrames.

    - sommets distincts deux à deux ;
    - `temporal` : les virements se suivent dans le temps (e1 < e2 < ... < en) et
      la boucle tient dans la fenêtre. C'est la signature d'un flux d'argent réel ;
      cela garantit aussi qu'un cycle n'est rendu qu'une fois (une seule rotation) ;
    - sans `temporal` (ablation), on garde la rotation dont e1 a le plus petit id ;
    - `only_new` : au moins une arête du cycle appartient au micro-batch courant
      (recherche incrémentale : chaque cycle est signalé une seule fois).
    """
    m = graph(edges).find(motif_cycle(n))
    vs = [f"v{i}" for i in range(1, n + 1)]
    for i in range(n):
        for j in range(i + 1, n):
            m = m.filter(F.col(f"{vs[i]}.id") != F.col(f"{vs[j]}.id"))
    if temporal:
        for i in range(1, n):
            m = m.filter(F.col(f"e{i}.event_time") < F.col(f"e{i + 1}.event_time"))
        m = m.filter(_seconds(f"e{n}.event_time") - _seconds("e1.event_time") <= window_seconds)
    else:
        for i in range(2, n + 1):
            m = m.filter(F.col("e1.transaction_id") < F.col(f"e{i}.transaction_id"))
    if only_new:
        cond = F.col("e1.is_new")
        for i in range(2, n + 1):
            cond = cond | F.col(f"e{i}.is_new")
        m = m.filter(cond)
    return _flatten(m, n)


def candidate_edges(window_edges: DataFrame, new_ids: DataFrame) -> DataFrame:
    """Sous-graphe minimal pour la recherche incrémentale de triangles.

    Un triangle contenant une arête nouvelle u->v est fermé par v->w et w->u :
      - v->w part d'un bénéficiaire d'une arête nouvelle ;
      - w->u arrive sur un émetteur d'une arête nouvelle ;
      - le sommet w est commun aux deux ensembles (semi-jointures d'élagage).
    Le motif GraphFrames ne tourne donc que sur quelques dizaines d'arêtes :
    le coût suit la taille du micro-batch, pas celle de la fenêtre de 10 minutes.
    """
    new = window_edges.join(F.broadcast(new_ids), "transaction_id", "left_semi")
    heads = new.select(F.col("dst").alias("v")).distinct()
    tails = new.select(F.col("src").alias("u")).distinct()
    out_v = window_edges.join(F.broadcast(heads), window_edges.src == heads.v, "left_semi")
    in_u = window_edges.join(F.broadcast(tails), window_edges.dst == tails.u, "left_semi")
    # Petits ensembles matérialisés une fois : le motif les relit plusieurs fois.
    out_v, in_u = out_v.localCheckpoint(eager=True), in_u.localCheckpoint(eager=True)
    w = out_v.select(F.col("dst").alias("w")).intersect(in_u.select(F.col("src").alias("w")))
    out_v = out_v.join(F.broadcast(w), out_v.dst == w.w, "left_semi")
    in_u = in_u.join(F.broadcast(w), in_u.src == w.w, "left_semi")
    cand = new.unionByName(out_v).unionByName(in_u).dropDuplicates(["transaction_id"])
    cand = cand.join(F.broadcast(new_ids.withColumn("is_new", F.lit(True))), "transaction_id", "left")
    # Matérialisation : sans elle, GraphFrames recalculerait toute la chaîne pour
    # chaque arête et chaque sommet du motif (des dizaines de jobs par micro-batch).
    return cand.fillna(False, ["is_new"]).localCheckpoint(eager=True)


def frame_from_rows(spark, rows: list[dict], with_is_new: set[str] | None = None) -> DataFrame:
    """Petit DataFrame d'arêtes construit depuis le driver via Arrow, puis matérialisé
    (localCheckpoint) : les données vivent dans la JVM et le motif GraphFrames utilise
    des jointures broadcast. ~0,1 s contre ~1 s par le chemin Python classique."""
    import pandas as pd

    pdf = pd.DataFrame([{f: r.get(f) for f in EDGE_FIELDS} for r in rows], columns=EDGE_FIELDS, dtype=object)
    schema = ("transaction_id STRING, src STRING, dst STRING, montant DOUBLE, event_time TIMESTAMP, "
              "type_operation STRING, kafka_timestamp TIMESTAMP")
    if with_is_new is not None:
        pdf["is_new"] = pdf["transaction_id"].isin(with_is_new)
        schema += ", is_new BOOLEAN"
    return spark.createDataFrame(pdf, schema).coalesce(1).localCheckpoint(eager=True)


def rows_to_cycle_edges(row, n: int) -> list[dict]:
    d = row.asDict()
    return [
        {"transaction_id": d[f"e{i}_transaction_id"], "src": d[f"e{i}_src"], "dst": d[f"e{i}_dst"],
         "montant": d[f"e{i}_montant"], "event_time": d[f"e{i}_event_time"],
         "type_operation": d[f"e{i}_type_operation"], "kafka_timestamp": d[f"e{i}_kafka_timestamp"]}
        for i in range(1, n + 1)
    ]


# --------------------------------------------------------------------------- centralité & composantes
def _pairs(edges: DataFrame) -> DataFrame:
    return edges.select("src", "dst").distinct()


def pagerank(edges: DataFrame, max_iter: int = 10) -> DataFrame:
    """PageRank (id, pagerank, pagerank_pct) sur le graphe agrégé des paires."""
    g = graph(_pairs(edges))
    pr = g.pageRank(resetProbability=0.15, maxIter=max_iter).vertices
    return pr.withColumn("pagerank_pct", F.percent_rank().over(Window.orderBy("pagerank")))


def trim_acyclic(pairs: DataFrame, rounds: int = 4) -> DataFrame:
    """Élagage (« trimming ») : un compte sans arête entrante ou sans arête sortante
    ne peut appartenir à aucun cycle. On le retire, itérativement. Il ne reste que
    le noyau susceptible de contenir des cycles, sur lequel la SCC est très rapide."""
    pairs = pairs.localCheckpoint(eager=True)
    for _ in range(rounds):
        both = (pairs.select(F.col("src").alias("x")).distinct()
                .intersect(pairs.select(F.col("dst").alias("x")).distinct()))
        # matérialisation à chaque tour : sinon le plan logique double à chaque itération
        nxt = (pairs.join(F.broadcast(both.withColumnRenamed("x", "src")), "src", "left_semi")
               .join(F.broadcast(both.withColumnRenamed("x", "dst")), "dst", "left_semi")
               .localCheckpoint(eager=True))
        release(pairs)
        pairs = nxt
    return pairs


def strongly_connected(edges: DataFrame, max_iter: int = 5) -> DataFrame:
    """Composantes fortement connexes : tout cycle orienté est inclus dans une SCC.
    Calculées sur le noyau élagué ; les comptes élagués sont des SCC singletons."""
    core = trim_acyclic(_pairs(edges))
    if core.isEmpty():
        release(core)
        return edges.sparkSession.createDataFrame([], "id STRING, scc_id STRING, scc_size INT")
    comp = graph(core).stronglyConnectedComponents(maxIter=max_iter)
    sizes = comp.groupBy("component").agg(F.count("*").alias("scc_size"))
    out = (comp.join(sizes, "component").select(
        "id", F.col("component").cast("string").alias("scc_id"), F.col("scc_size").cast("int"))
        .localCheckpoint(eager=True))
    release(core)
    return out


def connected_components(edges: DataFrame, algorithm: str = "graphx") -> DataFrame:
    """Composantes connexes (faibles). « graphx » est le plus rapide sur les petits
    graphes suspects ; « graphframes » passe mieux à l'échelle sur un cluster."""
    comp = graph(_pairs(edges)).connectedComponents(algorithm=algorithm)
    return comp.select("id", F.col("component").cast("string").alias("cluster_id"))


def long_cycles_in_sccs(hv_edges: DataFrame, scc: DataFrame, lengths=(4, 5),
                        window_seconds: int = 600) -> dict[int, list]:
    """Cycles de longueur 4 et 5, cherchés uniquement à l'intérieur des SCC assez
    grandes : on évite l'explosion combinatoire d'un motif long sur tout le graphe."""
    out = {}
    for n in lengths:
        members = scc.filter(F.col("scc_size") >= n).select("id", "scc_id")
        s = members.withColumnRenamed("id", "src").withColumnRenamed("scc_id", "scc_src")
        d = members.withColumnRenamed("id", "dst").withColumnRenamed("scc_id", "scc_dst")
        sub = (hv_edges.join(F.broadcast(s), "src").join(F.broadcast(d), "dst")
               .filter("scc_src = scc_dst").drop("scc_src", "scc_dst").localCheckpoint(eager=True))
        if sub.isEmpty():
            release(sub)
            continue
        out[n] = find_cycles(sub, n, window_seconds, temporal=True).collect()
        release(sub)
    return out


# --------------------------------------------------------------------------- autres typologies
def find_mules(edges: DataFrame, min_senders: int, min_inflow: float, flow_min: float,
               burst_minutes: int = 6, history: DataFrame | None = None, history_minutes: int = 20,
               spike_ratio: float = 3.0) -> DataFrame:
    """Comptes de passage (smurfing puis retrait). On repère le plus gros virement
    sortant de chaque compte (le « retrait ») et on regarde les dépôts qui le précèdent :
      - rafale : au moins `min_senders` émetteurs distincts et `min_inflow` MAD dans les
        `burst_minutes` minutes avant le retrait ;
      - pic anormal (si `history` est fourni) : cette rafale compte au moins `spike_ratio`
        fois plus de dépôts que le rythme habituel du compte sur les `history_minutes`
        minutes précédentes. Un commerçant reçoit en continu : pas de pic ;
      - le retrait reprend `flow_min` à 102 % de la rafale (vidage du compte).
    Un compte qui paie plus qu'il n'a reçu, ou avant de recevoir, puise dans un solde
    existant : c'est un compte actif légitime. Un paiement légitime reçu après le
    retrait ne masque pas la mule."""
    big = edges.groupBy(F.col("src").alias("account")).agg(
        F.sum("montant").alias("outflow"), F.max("montant").alias("max_out"),
        F.max_by("event_time", "montant").alias("big_out_time"))
    before = (edges.join(big, edges.dst == big.account)
              .filter(F.col("event_time") < F.col("big_out_time"))
              .withColumn("in_burst", F.col("event_time")
                          >= F.col("big_out_time") - F.expr(f"INTERVAL {burst_minutes} MINUTES")))
    burst = F.col("in_burst")
    inc = before.groupBy("account").agg(
        F.countDistinct(F.when(burst, F.col("src"))).alias("n_senders"),
        F.sum(F.when(burst, 1).otherwise(0)).alias("n_deposits"),
        F.sum(F.when(burst, F.col("montant")).otherwise(0.0)).alias("inflow"),
        F.collect_list(F.when(burst, F.col("montant"))).alias("incoming"),
        F.min(F.when(burst, F.col("event_time"))).alias("first_in"),
        F.max(F.when(burst, F.col("event_time"))).alias("last_in"))
    cand = (inc.join(big, "account")
            .filter((F.col("n_senders") >= min_senders) & (F.col("inflow") >= min_inflow)
                    & (F.col("max_out") >= flow_min * F.col("inflow"))
                    & (F.col("max_out") <= 1.02 * F.col("inflow"))))
    if history is None:
        return cand.withColumn("baseline_per_burst", F.lit(None).cast("double"))
    start = history.agg(F.min("event_time")).first()[0]
    if start is None:
        return cand.limit(0).withColumn("baseline_per_burst", F.lit(None).cast("double"))
    lo = F.expr(f"INTERVAL {burst_minutes + history_minutes} MINUTES")
    hi = F.expr(f"INTERVAL {burst_minutes} MINUTES")
    keys = cand.select(F.col("account").alias("acc"), F.col("big_out_time").alias("t_out"))
    base = (history.join(F.broadcast(keys), history.dst == keys.acc)
            .filter((F.col("event_time") >= F.col("t_out") - lo) & (F.col("event_time") < F.col("t_out") - hi))
            .groupBy("acc").agg(F.count("*").alias("baseline_count")))
    # Historique réellement disponible avant la rafale (démarrage à froid : on ne juge pas)
    hist_from = F.greatest(F.col("big_out_time") - lo, F.lit(start))
    hist_minutes = ((F.col("big_out_time") - hi).cast("double") - hist_from.cast("double")) / 60
    return (cand.join(base, cand.account == base.acc, "left").drop("acc").fillna(0, ["baseline_count"])
            .withColumn("history_minutes", hist_minutes)
            .filter(F.col("history_minutes") >= burst_minutes)
            .withColumn("baseline_per_burst", F.col("baseline_count") * burst_minutes / F.col("history_minutes"))
            .filter(F.col("n_deposits") >= spike_ratio * F.greatest(F.col("baseline_per_burst"), F.lit(1.0))))


def find_scatter_gather(edges: DataFrame, min_k: int, window_seconds: int,
                        min_amount: float = 5_000.0) -> DataFrame:
    """Motif à 2 sauts (s)->(m)->(t) : une même source et une même destination
    reliées par au moins `min_k` intermédiaires qui reversent 90-100 % du montant."""
    e = edges.filter(F.col("montant") >= min_amount)
    m = (graph(e).find("(s)-[e1]->(m); (m)-[e2]->(t)")
         .filter((F.col("s.id") != F.col("t.id")) & (F.col("s.id") != F.col("m.id"))
                 & (F.col("m.id") != F.col("t.id")))
         .filter(F.col("e1.event_time") < F.col("e2.event_time"))
         .filter(_seconds("e2.event_time") - _seconds("e1.event_time") <= window_seconds)
         .filter((F.col("e2.montant") <= F.col("e1.montant"))
                 & (F.col("e2.montant") >= 0.9 * F.col("e1.montant"))))
    return (m.groupBy(F.col("s.id").alias("source"), F.col("t.id").alias("sink"))
            .agg(F.countDistinct("m.id").alias("k"),
                 F.collect_list(F.struct(*[F.col(f"e1.{f}").alias(f) for f in EDGE_FIELDS])).alias("out_edges"),
                 F.collect_list(F.struct(*[F.col(f"e2.{f}").alias(f) for f in EDGE_FIELDS])).alias("in_edges"))
            .filter(F.col("k") >= min_k))
