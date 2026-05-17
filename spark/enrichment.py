"""
spark/enrichment.py

Spark Structured Streaming — Enrichment job (Phase 3 Block 2).

Reads : ride-events-raw      (Kafka, JSON, keyed by PULocationID)
Writes: ride-events-enriched (Kafka, JSON, keyed by PULocationID)

Adds:
  - Pickup zone metadata     (name, borough, centroid lat/lon)
  - Dropoff zone metadata    (name, borough, centroid lat/lon)
  - Temporal features        (hour_of_day, day_of_week, is_weekend, is_rush_hour)
  - NYC-local timestamp string for human-readable consumers

Stateless: no watermark, no aggregation, no stream-stream join.
Block 3 will add the weather stream-stream join on top of this output.

Time-zone semantics
-------------------
The TLC `pickup_datetime` field is local NYC wall-clock time. The replay
producer publishes it as ISO-8601 with timezone offset, so Spark parses
to a UTC instant on read. ALL temporal features (hour_of_day, is_rush_hour,
etc.) are computed in America/New_York via the session timezone set in
_session.py. The output also carries `pickup_datetime_nyc` as a formatted
string for downstream consumers that don't want to deal with timezones.

Run modes
---------
Host (fast dev — needs zones already in Postgres):

    python spark/enrichment.py

Docker (prod-shape):

    docker compose exec spark-master /opt/spark/bin/spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \\
        /app/spark/enrichment.py

The `org.postgresql:postgresql` package is needed for the JDBC zone load.
The Spark-Kafka package version MUST match the Spark version (3.5.1).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make scripts/_session.py importable when run as a standalone script.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame, SparkSession, functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from _session import checkpoint_dir, get_session, kafka_brokers  # noqa: E402

# ---------------------------------------------------------------------------
# Config (env-driven so docker-compose can override without code changes)
# ---------------------------------------------------------------------------
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "ride-events-raw")
SINK_TOPIC = os.environ.get("SINK_TOPIC", "ride-events-enriched")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")
QUERY_NAME = "enrichment"

# Postgres connection for the static zones lookup. Defaults match docker-compose.
PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://postgres:5432/rides")
PG_USER = os.environ.get("PG_USER", "rides")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "rides")

# ---------------------------------------------------------------------------
# Schema of the JSON value published to ride-events-raw by replay_producer.py
# (Phase 2 Block 2). EXACTLY 7 fields — the Phase 2 producer's contract.
# If you ever extend the producer (e.g. to include fares), update this AND
# scripts/replay_producer.py together.
# ---------------------------------------------------------------------------
RAW_SCHEMA = StructType([
    StructField("hvfhs_license_num", StringType(),    True),
    StructField("pickup_datetime",   TimestampType(), True),
    StructField("dropoff_datetime",  TimestampType(), True),
    StructField("PULocationID",      IntegerType(),   True),
    StructField("DOLocationID",      IntegerType(),   True),
    StructField("trip_miles",        DoubleType(),    True),
    StructField("trip_time",         DoubleType(),    True),  # seconds
])


# ---------------------------------------------------------------------------
# Zone broadcast helpers
# ---------------------------------------------------------------------------

def load_zones_df(spark: SparkSession) -> DataFrame:
    """Load the 263 NYC taxi zones from Postgres, ready to be broadcast.

    The zone table is static for the lifetime of a streaming job. We load it
    once at startup; if the underlying table changes, the job needs a restart
    to pick up new zones.

    Tests can bypass this by providing their own DataFrame to `enrich()`.
    """
    return (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option(
            "dbtable",
            "(SELECT zone_id, zone_name, borough, "
            "        centroid_lat, centroid_lon "
            "   FROM taxi_zones) AS z",
        )
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def make_zone_lookups(zones: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Project the zones DF into pu_/do_ aliased lookups for joining."""
    pu_zones = F.broadcast(zones.select(
        F.col("zone_id").alias("pu_zone_id"),
        F.col("zone_name").alias("pu_zone_name"),
        F.col("borough").alias("pu_borough"),
        F.col("centroid_lat").alias("pu_centroid_lat"),
        F.col("centroid_lon").alias("pu_centroid_lon"),
    ))
    do_zones = F.broadcast(zones.select(
        F.col("zone_id").alias("do_zone_id"),
        F.col("zone_name").alias("do_zone_name"),
        F.col("borough").alias("do_borough"),
        F.col("centroid_lat").alias("do_centroid_lat"),
        F.col("centroid_lon").alias("do_centroid_lon"),
    ))
    return pu_zones, do_zones


# ---------------------------------------------------------------------------
# Core enrichment — pure DataFrame -> DataFrame transform.
#
# Extracted into a function so unit tests can call it with a static DF
# instead of a streaming source. Same code path runs in production.
# ---------------------------------------------------------------------------

def enrich(parsed: DataFrame, zones: DataFrame) -> DataFrame:
    """Add temporal features and zone metadata to a parsed trips DataFrame.

    Args:
        parsed: A DataFrame with the columns from RAW_SCHEMA.
        zones:  Taxi zones DataFrame (zone_id, zone_name, borough, centroid_lat,
                centroid_lon). Will be broadcast.

    Returns: Enriched DataFrame ready for serialization.
    """
    pu_zones, do_zones = make_zone_lookups(zones)

    # --- Drop rows that can't be enriched at all ---
    cleaned = parsed.where(
        F.col("pickup_datetime").isNotNull() & F.col("PULocationID").isNotNull()
    )

    # --- Temporal features (computed in America/New_York via session tz) ---
    # dayofweek():    1 = Sunday ... 7 = Saturday  (matches schema SMALLINT)
    # is_weekend:     Sat or Sun
    # is_rush_hour:   weekday 07-09 OR 16-19  (NYC commute pattern)
    with_temporal = (
        cleaned
        .withColumn("hour_of_day", F.hour("pickup_datetime").cast("smallint"))
        .withColumn("day_of_week", F.dayofweek("pickup_datetime").cast("smallint"))
        .withColumn("is_weekend",  F.col("day_of_week").isin(1, 7))
        .withColumn(
            "is_rush_hour",
            (~F.col("is_weekend")) & (
                F.col("hour_of_day").between(7, 9)
                | F.col("hour_of_day").between(16, 19)
            ),
        )
        # Human-readable NYC-local timestamp strings for downstream consumers
        # that don't want to deal with timezones.
        .withColumn(
            "pickup_datetime_nyc",
            F.date_format("pickup_datetime", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "dropoff_datetime_nyc",
            F.date_format("dropoff_datetime", "yyyy-MM-dd HH:mm:ss"),
        )
    )

    # --- Broadcast joins on zones ---
    # LEFT joins so we never drop a trip just because of a zone lookup miss
    # (e.g. zone_id 264/265 = "Unknown" / "Outside of NYC").
    enriched = (
        with_temporal
        .join(pu_zones, with_temporal["PULocationID"] == pu_zones["pu_zone_id"], "left")
        .join(do_zones, with_temporal["DOLocationID"] == do_zones["do_zone_id"], "left")
    )
    return enriched


def to_kafka_payload(enriched: DataFrame) -> DataFrame:
    """Serialize the enriched DF into Kafka-ready (key, value) pairs.

    Re-keys by PULocationID so downstream Aggregation gets zone-level
    partition locality (same-zone trips co-partition, no shuffle on groupBy).
    """
    return enriched.select(
        F.col("PULocationID").cast("string").alias("key"),
        F.to_json(F.struct(
            # raw fields preserved
            "hvfhs_license_num",
            "pickup_datetime",
            "dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_miles",
            "trip_time",
            # NYC-local timestamps (human-readable)
            "pickup_datetime_nyc",
            "dropoff_datetime_nyc",
            # pickup zone metadata
            "pu_zone_id", "pu_zone_name", "pu_borough",
            "pu_centroid_lat", "pu_centroid_lon",
            # dropoff zone metadata
            "do_zone_id", "do_zone_name", "do_borough",
            "do_centroid_lat", "do_centroid_lon",
            # temporal features (NYC-local)
            "hour_of_day", "day_of_week", "is_weekend", "is_rush_hour",
        )).alias("value"),
    )


# ---------------------------------------------------------------------------
# main: wire up the streaming source, transform, and sink
# ---------------------------------------------------------------------------

def main() -> int:
    spark = get_session(QUERY_NAME)
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print(f"  Spark master:    {spark.sparkContext.master}")
    print(f"  Kafka brokers:   {kafka_brokers()}")
    print(f"  Source topic:    {SOURCE_TOPIC}")
    print(f"  Sink topic:      {SINK_TOPIC}")
    print(f"  Trigger:         {TRIGGER_INTERVAL}")
    print(f"  Postgres URL:    {PG_URL}")
    print(f"  Checkpoint:      {checkpoint_dir(QUERY_NAME)}")
    print("=" * 70)

    # --- Static dimension: zones (loaded once at startup) ---
    zones = load_zones_df(spark).cache()
    n_zones = zones.count()
    print(f"\n  Loaded {n_zones} taxi zones from Postgres.")
    if n_zones < 260:
        print(f"  WARNING: expected ~263 zones, got {n_zones}. "
              f"Did you run scripts/load_zones.py?")

    # --- Streaming source ---
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        # Dev-cluster choice: tolerate offset gaps from `create_topics.py --reset`.
        # In production this would be 'true' to surface real data loss.
        .option("failOnDataLoss", "false")
        .load()
    )

    # --- Parse JSON -> typed DataFrame ---
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("e"))
        .select("e.*")
    )

    # --- Transform ---
    enriched = enrich(parsed, zones)

    # --- Serialize for Kafka ---
    out = to_kafka_payload(enriched)

    # --- Sink to ride-events-enriched ---
    query = (
        out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("topic", SINK_TOPIC)
        .option("checkpointLocation", checkpoint_dir(QUERY_NAME))
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(QUERY_NAME)
        .start()
    )

    print(f"\nStreaming query '{query.name}' started.")
    print("  → Run replay_producer.py in another terminal to feed ride-events-raw.")
    print("  → Spark UI: http://localhost:4040 (host) or :8082 (docker)")
    print("  → Press Ctrl-C to stop.\n")

    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
