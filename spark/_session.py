"""
Shared SparkSession factory for Phase 3 streaming jobs.

This module exists so every Spark job we write can be run two ways
without code changes:

    Host mode (fast dev iteration):
        python spark/hello_kafka.py
        # → master=local[*], reads .venv packages, JVM starts in seconds

    Docker mode (prod-shape demo):
        docker compose exec spark-master spark-submit \\
            --master spark://spark-master:7077 \\
            /app/spark/hello_kafka.py
        # → master=spark cluster, jobs distributed across spark-worker-{1,2}

The mode is selected by environment variables:

    SPARK_MASTER     - "local[*]" (host) or "spark://spark-master:7077" (docker)
    KAFKA_BROKERS    - "localhost:9092,..." (host) or "kafka-1:19092,..." (docker)
    CHECKPOINT_DIR   - "./checkpoints" (host) or "/checkpoints" (docker)

In host mode, defaults work out of the box. In docker mode, the values are
injected by docker-compose.yml.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession


# Pinned Maven coordinates. The Kafka package version MUST match the Spark
# version exactly — kafka-0-10 means "Kafka client API 0.10+", which is
# what current Kafka brokers speak. Don't be misled by the "0.10" suffix —
# it works against Kafka 3.x cluster fine.
SPARK_VERSION = "3.5.1"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"


def get_session(app_name: str) -> SparkSession:
    """Build (or return the existing) SparkSession for a streaming job.

    Master selection priority:
      1. If SPARK_MASTER env var is set explicitly, use it.
      2. Else if launched by spark-submit (detected via PYSPARK_GATEWAY_PORT,
         which is only present when the JVM was spawned by spark-submit),
         defer entirely to spark-submit's --master and --packages flags.
      3. Else (plain `python spark/foo.py` on host), default to local[*]
         and pull the Kafka JAR via Maven.
    """
    explicit_master = os.environ.get("SPARK_MASTER")
    launched_by_submit = "PYSPARK_GATEWAY_PORT" in os.environ

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.adaptive.enabled", "false")
    )

    if explicit_master:
        builder = builder.master(explicit_master)
        if explicit_master.startswith("local"):
            builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    elif launched_by_submit:
        # spark-submit's --master and --packages flags will be honored.
        # Don't call .master() or set spark.jars.packages — would override.
        pass
    else:
        # Host mode: default to local[*] and pull the Kafka JAR via Maven.
        builder = builder.master("local[*]").config("spark.jars.packages", KAFKA_PACKAGE)

    return builder.getOrCreate()


def kafka_brokers() -> str:
    """Bootstrap servers for whichever environment we're running in."""
    return os.environ.get(
        "KAFKA_BROKERS",
        "localhost:9092,localhost:9093,localhost:9094",
    )


def checkpoint_dir(query_name: str) -> str:
    """Return the per-query checkpoint location.

    Host mode:    ./checkpoints/<query_name>
    Docker mode:  /checkpoints/<query_name>   (mounted volume)

    Each streaming query MUST have a unique checkpoint dir — sharing them
    silently corrupts state.
    """
    base = os.environ.get("CHECKPOINT_DIR", "./checkpoints")
    path = Path(base) / query_name
    path.mkdir(parents=True, exist_ok=True)
    return str(path)
