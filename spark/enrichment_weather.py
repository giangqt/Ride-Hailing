#!/usr/bin/env python3
"""
spark/enrichment_weather.py

Spark Structured Streaming — Weather enrichment job (Phase 3 Block 3).

Reads : ride-events-enriched   (Kafka, JSON, output of Block 2)
        weather-events         (Kafka, JSON, hourly observations)
Writes: ride-events-enriched-weather

This is the first STATEFUL job in the pipeline. The two-stream join needs:
  - watermarks on both sides to bound state growth
  - a time-bounded join condition so Spark knows when to discard unmatched events
  - output mode 'append' (only emit rows past the watermark)

Why a separate topic instead of modifying Block 2's output:
  - Block 2 stays as-is and provably working
  - Each stage's output is independently inspectable
  - If weather collection breaks, trip enrichment continues to flow
  - Easier to reason about independently in tests

Stream-stream join semantics
----------------------------
A trip pickup at 14:23 should be matched with the weather observation valid
for hour 14:00. Spark's stream-stream join requires an EQUALITY predicate
(it can't bound state on inequalities alone — Spark refuses such queries
at planning time). So we derive an "hour bucket" column on both sides via
date_trunc and join on equality of buckets.

Concretely, the join condition is:

    date_trunc("hour", trip.pickup_datetime) = date_trunc("hour", weather.observation_time)

Earlier drafts of this code tried a time-bound condition like
``weather.time BETWEEN trip.time - 30min AND trip.time + 1h``. Spark
rejects those at planning time:
``Stream-stream join without equality predicate is not supported``.

Hour-bucket equi-join is also more semantically honest: weather is
hourly observations, so matching trips to "the weather observation
valid for their hour" is exactly the right thing.

Watermarks
----------
trips:   pickup_datetime - 30 minutes
         (we'll wait 30 min for late trips before considering a window done)
weather: observation_time - 2 hours
         (weather is hourly, generous tolerance to handle publisher delays)

The OUTPUT delay equals the larger of these two watermarks (~2 hours from
event time). If you need tighter latency, shorten the watermarks at the cost
of dropping more late events.

Run modes
---------
Host:     PG_URL='jdbc:postgresql://localhost:5432/rides' python spark/enrichment_weather.py
Docker:   docker compose exec spark-master /opt/spark/bin/spark-submit \\
            --master spark://spark-master:7077 \\
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
            /app/spark/enrichment_weather.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame, functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from _session import checkpoint_dir, get_session, kafka_brokers  # noqa: E402

# ---------------------------------------------------------------------------
# Config (env-driven)
# ---------------------------------------------------------------------------
TRIPS_TOPIC   = os.environ.get("TRIPS_TOPIC",   "ride-events-enriched")
WEATHER_TOPIC = os.environ.get("WEATHER_TOPIC", "weather-events")
SINK_TOPIC    = os.environ.get("SINK_TOPIC",    "ride-events-enriched-weather")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
QUERY_NAME = "enrichment_weather"

# Watermark tuning. See module docstring for trade-offs.
TRIPS_WATERMARK   = "30 minutes"
WEATHER_WATERMARK = "2 hours"

# ---------------------------------------------------------------------------
# Schemas — these MUST match what the upstream producers actually emit.
# ride-events-enriched: from spark/enrichment.py (Block 2 output)
# weather-events:       from scripts/fetch_weather.py (Phase 2 Block 3)
# ---------------------------------------------------------------------------

ENRICHED_TRIP_SCHEMA = StructType([
    # raw (from replay_producer.py)
    StructField("hvfhs_license_num",   StringType(),    True),
    StructField("pickup_datetime",     TimestampType(), True),
    StructField("dropoff_datetime",    TimestampType(), True),
    StructField("PULocationID",        IntegerType(),   True),
    StructField("DOLocationID",        IntegerType(),   True),
    StructField("trip_miles",          DoubleType(),    True),
    StructField("trip_time",           DoubleType(),    True),
    # NYC-local timestamp strings (from Block 2)
    StructField("pickup_datetime_nyc", StringType(), True),
    StructField("dropoff_datetime_nyc", StringType(), True),
    # zone metadata (from Block 2)
    StructField("pu_zone_id",      IntegerType(), True),
    StructField("pu_zone_name",    StringType(),  True),
    StructField("pu_borough",      StringType(),  True),
    StructField("pu_centroid_lat", DoubleType(),  True),
    StructField("pu_centroid_lon", DoubleType(),  True),
    StructField("do_zone_id",      IntegerType(), True),
    StructField("do_zone_name",    StringType(),  True),
    StructField("do_borough",      StringType(),  True),
    StructField("do_centroid_lat", DoubleType(),  True),
    StructField("do_centroid_lon", DoubleType(),  True),
    # temporal features (from Block 2)
    StructField("hour_of_day",  IntegerType(), True),
    StructField("day_of_week",  IntegerType(), True),
    StructField("is_weekend",   BooleanType(), True),
    StructField("is_rush_hour", BooleanType(), True),
])

WEATHER_SCHEMA = StructType([
    StructField("observation_time",   TimestampType(), True),
    StructField("station_id",         StringType(),    True),
    StructField("temperature_c",      DoubleType(),    True),
    StructField("precipitation_mm",   DoubleType(),    True),
    StructField("wind_speed_ms",      DoubleType(),    True),
    StructField("humidity_pct",       DoubleType(),    True),
    StructField("weather_condition",  StringType(),    True),
])


# ---------------------------------------------------------------------------
# Core join — pure DataFrame -> DataFrame transform, factored out for testing
# ---------------------------------------------------------------------------

def join_trips_with_weather(trips: DataFrame, weather: DataFrame) -> DataFrame:
    """Stream-stream join with watermarks on the hour-bucket join keys.

    Spark requires the watermark to be ON the join key (or alternatively on
    the nullable side + a range condition). The error message Spark emits if
    watermarks are on different columns from the join keys is:

        Stream-stream LeftOuter join not supported without a watermark in the
        join keys, or a watermark on the nullable side and an appropriate
        range condition

    So we compute pickup_hour FIRST, then watermark pickup_hour. Same for
    weather. The hour bucket is still a timestamp value, just truncated.
    """
    trips_wm = (
        trips
        .withColumn("pickup_hour", F.date_trunc("hour", F.col("pickup_datetime")))
        .withWatermark("pickup_hour", TRIPS_WATERMARK)
    )
    weather_wm = (
        weather
        .withColumn("observation_hour",
                    F.date_trunc("hour", F.col("observation_time")))
        .withWatermark("observation_hour", WEATHER_WATERMARK)
    )

    joined = trips_wm.alias("t").join(
        weather_wm.alias("w"),
        F.expr("t.pickup_hour = w.observation_hour"),
        "left",
    )
    return joined


def to_kafka_payload(joined: DataFrame) -> DataFrame:
    """Serialize the joined DF into Kafka-ready (key, value) pairs.

    Re-keys by PULocationID (same as Block 2) so downstream Aggregation
    keeps zone-level partition locality.
    """
    return joined.select(
        F.col("t.PULocationID").cast("string").alias("key"),
        F.to_json(F.struct(
            # all original enriched fields
            F.col("t.hvfhs_license_num").alias("hvfhs_license_num"),
            F.col("t.pickup_datetime").alias("pickup_datetime"),
            F.col("t.dropoff_datetime").alias("dropoff_datetime"),
            F.col("t.PULocationID").alias("PULocationID"),
            F.col("t.DOLocationID").alias("DOLocationID"),
            F.col("t.trip_miles").alias("trip_miles"),
            F.col("t.trip_time").alias("trip_time"),
            F.col("t.pickup_datetime_nyc").alias("pickup_datetime_nyc"),
            F.col("t.dropoff_datetime_nyc").alias("dropoff_datetime_nyc"),
            F.col("t.pu_zone_id").alias("pu_zone_id"),
            F.col("t.pu_zone_name").alias("pu_zone_name"),
            F.col("t.pu_borough").alias("pu_borough"),
            F.col("t.pu_centroid_lat").alias("pu_centroid_lat"),
            F.col("t.pu_centroid_lon").alias("pu_centroid_lon"),
            F.col("t.do_zone_id").alias("do_zone_id"),
            F.col("t.do_zone_name").alias("do_zone_name"),
            F.col("t.do_borough").alias("do_borough"),
            F.col("t.do_centroid_lat").alias("do_centroid_lat"),
            F.col("t.do_centroid_lon").alias("do_centroid_lon"),
            F.col("t.hour_of_day").alias("hour_of_day"),
            F.col("t.day_of_week").alias("day_of_week"),
            F.col("t.is_weekend").alias("is_weekend"),
            F.col("t.is_rush_hour").alias("is_rush_hour"),
            # weather fields (prefixed so they don't collide; null if no match)
            F.col("w.temperature_c").alias("weather_temperature_c"),
            F.col("w.precipitation_mm").alias("weather_precipitation_mm"),
            F.col("w.wind_speed_ms").alias("weather_wind_speed_ms"),
            F.col("w.humidity_pct").alias("weather_humidity_pct"),
            F.col("w.weather_condition").alias("weather_condition"),
            F.col("w.observation_time").alias("weather_observation_time"),
        ),
        # CRITICAL: emit null fields explicitly. Default behavior drops null
        # fields entirely, which means downstream jobs (Aggregation, Network,
        # PG Sink) see an unstable schema where a field's presence depends on
        # whether the join matched. We want every record to have all 30 keys,
        # with null where appropriate.
        {"ignoreNullFields": "false"}).alias("value"),
    )


# ---------------------------------------------------------------------------
# main: wire up the streaming sources, transform, and sink
# ---------------------------------------------------------------------------

def main() -> int:
    spark = get_session(QUERY_NAME)
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print(f"  Spark master:    {spark.sparkContext.master}")
    print(f"  Kafka brokers:   {kafka_brokers()}")
    print(f"  Trips topic:     {TRIPS_TOPIC}")
    print(f"  Weather topic:   {WEATHER_TOPIC}")
    print(f"  Sink topic:      {SINK_TOPIC}")
    print(f"  Trigger:         {TRIGGER_INTERVAL}")
    print(f"  Trips watermark: {TRIPS_WATERMARK}")
    print(f"  Weather watermark: {WEATHER_WATERMARK}")
    print(f"  Checkpoint:      {checkpoint_dir(QUERY_NAME)}")
    print("=" * 70)

    # --- Trips stream ---
    trips_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("subscribe", TRIPS_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    trips = (
        trips_raw
        .select(F.from_json(F.col("value").cast("string"),
                            ENRICHED_TRIP_SCHEMA).alias("e"))
        .select("e.*")
    )

    # --- Weather stream ---
    weather_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("subscribe", WEATHER_TOPIC)
        # Read all weather history on first start, then track latest.
        # Weather is low-volume (1 msg/hour), so reading from earliest is cheap
        # and lets the join match older trips on first run.
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    weather = (
        weather_raw
        .select(F.from_json(F.col("value").cast("string"),
                            WEATHER_SCHEMA).alias("w"))
        .select("w.*")
    )

    # --- The join ---
    joined = join_trips_with_weather(trips, weather)
    out = to_kafka_payload(joined)

    # --- Sink ---
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
    print("  → Need both trips AND weather flowing for the join to emit rows.")
    print("  → Output is delayed by ~watermark duration (~2h from event time).")
    print("  → Spark UI: http://localhost:4040 (host) or :8082 (docker)")
    print("  → Press Ctrl-C to stop.\n")

    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())