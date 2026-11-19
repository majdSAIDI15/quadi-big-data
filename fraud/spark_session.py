"""Création de la SparkSession (Kafka + Delta Lake + GraphFrames).

Les jars sont résolus une fois pour toutes au build de l'image Docker
(`python -m fraud.spark_session --warmup`) puis chargés depuis le disque :
la démo fonctionne donc sans accès internet.
"""
from __future__ import annotations

import glob
import os
import sys

from .config import settings

CONF_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "conf")
FAIR_FILE = os.path.join(CONF_DIR, "fairscheduler.xml")
LOG4J_FILE = os.path.join(CONF_DIR, "log4j2.properties")

SPARK_VERSION = "3.5.7"
SCALA = "2.12"
PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA}:{SPARK_VERSION}",
    f"io.delta:delta-spark_{SCALA}:3.3.2",
    f"io.graphframes:graphframes-spark3_{SCALA}:0.12.3",
]


def build_spark(app_name: str, master: str | None = None, extra_conf: dict | None = None):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "false")  # micro-batches : plans simples
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.databricks.delta.properties.defaults.checkpointInterval", "20")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        # Nettoyage régulier des RDD/shuffles non référencés (job de streaming de longue durée)
        .config("spark.cleaner.periodicGC.interval", "2min")
        # Petits DataFrames construits sur le driver : transfert Arrow + jointures broadcast
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.sql.defaultSizeInBytes", str(8 * 1024 * 1024))
    )
    if os.path.exists(FAIR_FILE):
        builder = builder.config("spark.scheduler.allocation.file", FAIR_FILE)
    # Mémoire hors tas bornée (tampons Arrow/Kafka) : le conteneur a une limite stricte
    java_opts = "-XX:MaxDirectMemorySize=512m"
    if os.path.exists(LOG4J_FILE):
        java_opts += f" -Dlog4j.configurationFile=file:{LOG4J_FILE}"
    builder = builder.config("spark.driver.extraJavaOptions", java_opts)
    jars_dir = os.environ.get("SPARK_JARS_DIR")
    jars = sorted(glob.glob(os.path.join(jars_dir, "*.jar"))) if jars_dir else []
    if jars:
        builder = builder.config("spark.jars", ",".join(jars))
    else:
        builder = builder.config("spark.jars.packages", ",".join(PACKAGES))
        if os.environ.get("SPARK_IVY_DIR"):
            builder = builder.config("spark.jars.ivy", os.environ["SPARK_IVY_DIR"])
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ckpt = os.path.join(settings.checkpoint_dir, "graphframes")
    os.makedirs(ckpt, exist_ok=True)
    spark.sparkContext.setCheckpointDir(ckpt)
    return spark


if __name__ == "__main__" and "--warmup" in sys.argv:
    # Résout les dépendances Maven au build de l'image, puis vérifie que tout se charge.
    s = build_spark("warmup", master="local[1]")
    from graphframes import GraphFrame  # noqa: F401
    v = s.createDataFrame([("a",), ("b",)], ["id"])
    e = s.createDataFrame([("a", "b")], ["src", "dst"])
    print("GraphFrames OK, in-degrees:", GraphFrame(v, e).inDegrees.count())
    s.stop()
