"""
Phase 3 Block 5: Network OD-pair aggregation.

Reads:  ride-events-enriched-weather   (output of Block 3 — enrichment_weather.py)
Writes: network-flow-updates            (1-hour OD pair flow counts)

Pipeline:
    enriched-weather events
        │
        └── aggregate_od_flows:  1-hour tumbling window per (origin, dest),
                                 count(*) as trip_count
        │
        └── to_kafka_payload → network-flow-updates

Streaming semantics:
    Window: 1-hour tumbling on pickup_datetime
    Trigger: 30 minutes (matches natural cadence of 1-hour windows
             + 30-min watermark; finer triggers would just wake Spark
             up to find no closed windows)
    Watermark: 30 minutes (pipeline-wide convention, matching Blocks 3/4)

Scope split (per BLOCK_5_STATUS.md):
    Streaming (this job):       trip_count per OD pair per window
    Downstream PG Sink / SQL:   in_degree, out_degree (window functions on
                                  the materialized network_flows table)
    Phase 4 Colab batch:        betweenness centrality (NetworkX),
                                Louvain community detection

Why degrees are NOT computed in streaming:
    Spark Structured Streaming does not support general window functions
    (PARTITION BY ... SUM) on streams — only time-window aggregations via
    F.window(). Computing degrees via groupBy + stream-stream join was the
    alternative, but Block 4 demonstrated the watermark fragility of
    stream-stream joins on derived time keys. The cleanest architecture is
    to keep streaming focused on flow counts and compute derived metrics
    downstream where SQL window functions work correctly. The PG Sink job
    (Block 6) materializes network_flows with degrees computed via:
        SUM(trip_count) OVER (PARTITION BY time_window, origin_zone_id)
    at INSERT time.

Run modes:
    Host:    python spark/network.py
    Docker:  docker compose exec spark-master spark-submit \\
                 --master spark://spark-master:7077 \\
                 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
                 /app/spark/network.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

# --- Path bootstrap so `python spark/network.py` from project root works ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from spark._session import (  # noqa: E402
    get_session,
    kafka_brokers,
    checkpoint_dir,
)


# ===========================================================================
# Constants — referenced by tests, do not change without updating contract
# ===========================================================================

SOURCE_TOPIC = "ride-events-enriched-weather"
SINK_TOPIC = "network-flow-updates"

WINDOW_DURATION = "1 hour"            # tumbling on pickup_datetime
WATERMARK_DELAY = "30 minutes"        # pipeline-wide convention

# Trigger interval: 30 minutes in production (matches natural cadence of
# 1-hour windows + 30-min watermark). Override via env var for smoke/integration
# tests where we don't want to wait 30 wall-clock minutes between batches:
#   NETWORK_TRIGGER_INTERVAL="10 seconds" python spark/network.py
# The static test asserts the DEFAULT value (30 minutes); the env var is a
# runtime knob for testing, not a contract change.
TRIGGER_INTERVAL = "30 minutes"

# JSON payload field set (test contract — must match network_flows table,
# excluding `betweenness` and `in_degree`/`out_degree` which are computed
# downstream by the PG Sink / analytical layer via SQL window functions).
# See BLOCK_5_STATUS.md for the scope-split rationale.
PAYLOAD_FIELDS = [
    "time_window", "origin_zone_id", "dest_zone_id", "trip_count",
]


# Schema of Block 3's output (ride-events-enriched-weather). Network only needs
# 4 fields of the 30 Block 3 emits. We use the _nyc timestamp variants for the
# same reason Block 4 does — ISO-with-offset variant doesn't parse reliably.
# See BLOCK_4_STATUS.md for the full rationale.
ENRICHED_WEATHER_SCHEMA = T.StructType([
    T.StructField("pickup_datetime_nyc", T.TimestampType(), nullable=False),
    T.StructField("PULocationID", T.IntegerType(), nullable=True),
    T.StructField("DOLocationID", T.IntegerType(), nullable=True),
])


# ===========================================================================
# Pure transforms — testable in isolation on static DataFrames
# ===========================================================================

def aggregate_od_flows(events: DataFrame) -> DataFrame:
    """1-hour tumbling aggregation per OD pair.

    Filters events with null PULocationID or DOLocationID (mirrors the
    data-quality contract enforced in Block 4).

    Output schema:
        time_window TIMESTAMP    -- window start, inclusive [start, start+1h)
        origin_zone_id INT
        dest_zone_id INT
        trip_count INT
    """
    return (
        events
        .filter(
            F.col("pickup_datetime").isNotNull()
            & F.col("PULocationID").isNotNull()
            & F.col("DOLocationID").isNotNull()
        )
        .withWatermark("pickup_datetime", WATERMARK_DELAY)
        .groupBy(
            F.window(F.col("pickup_datetime"), WINDOW_DURATION).alias("w"),
            F.col("PULocationID").alias("origin_zone_id"),
            F.col("DOLocationID").alias("dest_zone_id"),
        )
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .select(
            F.col("w.start").alias("time_window"),
            F.col("origin_zone_id"),
            F.col("dest_zone_id"),
            F.col("trip_count").cast("int").alias("trip_count"),
        )
    )


def to_kafka_payload(flows: DataFrame) -> DataFrame:
    """Serialize OD-flow rows for Kafka. Key=origin_zone_id, value=JSON.

    ignoreNullFields=false: every PAYLOAD_FIELDS key appears in the JSON,
    even if NULL (defensive against future nullable schema additions).
    """
    payload_struct = F.struct(*[F.col(c) for c in PAYLOAD_FIELDS])
    return flows.select(
        F.col("origin_zone_id").cast("string").alias("key"),
        F.to_json(payload_struct, {"ignoreNullFields": "false"}).alias("value"),
    )


# ===========================================================================
# Streaming setup
# ===========================================================================

def read_enriched_weather_stream(spark: SparkSession) -> DataFrame:
    """Subscribe to ride-events-enriched-weather and parse JSON to typed cols.

    Reads the `_nyc` timestamp variant (plain yyyy-MM-dd HH:mm:ss string)
    and aliases it to pickup_datetime — same pattern as Block 4. The
    ISO-with-offset variant Block 3 also emits does not parse reliably via
    Spark's from_json on TimestampType. See BLOCK_4_STATUS.md.

    startingOffsets default is 'latest' (production: only process new events).
    Override via env var for smoke testing against existing topic data:
        NETWORK_STARTING_OFFSETS="earliest" python spark/network.py

    Does NOT attach a watermark here — aggregate_od_flows attaches it
    directly on pickup_datetime before the windowed groupBy.
    """
    starting_offsets = os.environ.get("NETWORK_STARTING_OFFSETS", "latest")
    print(f"[network] starting_offsets={starting_offsets}", flush=True)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.from_json(
            F.col("value").cast("string"),
            ENRICHED_WEATHER_SCHEMA,
        ).alias("e"))
        .select(
            F.col("e.pickup_datetime_nyc").alias("pickup_datetime"),
            F.col("e.PULocationID").alias("PULocationID"),
            F.col("e.DOLocationID").alias("DOLocationID"),
        )
    )
    return parsed


def main() -> int:
    spark = get_session(app_name="network")
    spark.sparkContext.setLogLevel("WARN")

    # Trigger interval can be overridden via env var for smoke/integration
    # tests. Production default is TRIGGER_INTERVAL (30 minutes). Override:
    #   NETWORK_TRIGGER_INTERVAL="10 seconds" python spark/network.py
    trigger_interval = os.environ.get("NETWORK_TRIGGER_INTERVAL", TRIGGER_INTERVAL)
    print(f"[network] trigger_interval={trigger_interval}", flush=True)

    events = read_enriched_weather_stream(spark)

    # Build the pipeline (lazy — no execution until the streaming query starts)
    flows = aggregate_od_flows(events)
    payload = to_kafka_payload(flows)

    query = (
        payload
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("topic", SINK_TOPIC)
        .option("checkpointLocation", checkpoint_dir("network"))
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())