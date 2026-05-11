"""
Phase 3 Block 4: Aggregation + hotspot detection.

Reads:  ride-events-enriched-weather   (output of Block 3 — enrichment_weather.py)
Writes: demand-per-zone                (every window: per-zone demand metrics)
        hotspot-alerts                 (only when ratio >= 2.0 AND baseline >= 1.0)

Pipeline:
    enriched-weather events
        │
        ├── aggregate_window:  15-min tumbling window per zone, counts and avgs
        │       (output: demand-per-zone)
        │
        ├── attach_baseline:   broadcast left-join with zone_demand_baseline.csv
        │                      on (zone_id, hour_of_week derived from window-start
        │                      in America/New_York). Computes:
        │                          demand_baseline = baseline_mean_pickups * 0.25
        │                          ratio = pickup_count / demand_baseline
        │                      ratio is NULL when baseline = 0 (mathematically
        │                      undefined, not lied-about as 0).
        │
        └── extract_hotspots:  filter where baseline_mean_pickups >= 1.0
                               AND ratio >= 2.0. Severity:
                                   2.0 <= ratio < 3.0 → "warning"
                                   ratio >= 3.0      → "critical"
                               (output: hotspot-alerts)

Time-bucket convention:
    time_bucket = window-START (inclusive lower bound).
    A row with time_bucket=14:30:00 covers the window [14:30:00, 14:45:00).

Watermark policy:
    30 minutes on the windowed event-time column. Pipeline-wide convention
    matching enrichment_weather.py — every stateful Spark job uses 30 min for
    consistency at thesis defense. Cost vs benefit: 45-min total alert latency
    (15-min window + 30-min watermark). Acceptable for academic correctness.

Run modes:
    Host:    python spark/aggregation.py
    Docker:  docker compose exec spark-master spark-submit \\
                 --master spark://spark-master:7077 \\
                 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
                 /app/spark/aggregation.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

# --- Path bootstrap so `python spark/aggregation.py` from project root works ---
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
DEMAND_TOPIC = "demand-per-zone"
HOTSPOT_TOPIC = "hotspot-alerts"

WINDOW_DURATION = "15 minutes"
WATERMARK_DELAY = "30 minutes"  # pipeline-wide convention

# Hotspot detection thresholds
BASELINE_HOTSPOT_FLOOR = 1.0      # zones with baseline < this don't emit alerts
SEVERITY_WARNING_RATIO = 2.0      # ratio threshold for "warning"
SEVERITY_CRITICAL_RATIO = 3.0     # ratio threshold for "critical"

# 15 min / 60 min = 0.25 — the per-window slice of an hourly baseline rate
HOURLY_TO_WINDOW_SCALE = 0.25

# JSON payload field sets (test contract — must match Postgres tables)
DEMAND_PAYLOAD_FIELDS = [
    "time_bucket", "zone_id", "pickup_count", "dropoff_count",
    "avg_trip_miles", "avg_trip_time", "avg_temperature", "precipitation_mm",
]
ALERT_PAYLOAD_FIELDS = [
    "detected_at", "zone_id", "demand_current", "demand_baseline",
    "ratio", "severity",
]

# Default baseline CSV location (overridable via env var for testing)
DEFAULT_BASELINE_CSV = str(PROJECT_ROOT / "data" / "baseline" / "zone_demand_baseline.csv")


# Schema of Block 3's output (ride-events-enriched-weather). 30 fields total
# in production; we only deserialize the 7 we need for aggregation.
#
# IMPORTANT — name translation across the block boundary:
#   Block 3 emits:                    Block 4 outputs (Postgres-aligned):
#     pickup_datetime_nyc      →        pickup_datetime (parsed timestamp)
#     dropoff_datetime_nyc     →        dropoff_datetime (parsed timestamp)
#     PULocationID            →         zone_id (in pickup-side aggregation)
#     DOLocationID            →         zone_id (in dropoff-side aggregation)
#     trip_time (seconds)     →         avg_trip_time (minutes; divide by 60)
#     weather_temperature_c   →         avg_temperature
#     weather_precipitation_mm →        precipitation_mm
#
# Why _nyc variants for timestamps: Block 3 also emits `pickup_datetime` and
# `dropoff_datetime` as ISO 8601 strings with timezone offsets (e.g.
# "2026-05-07T23:20:24.557-04:00"). Spark's default from_json on TimestampType
# does not reliably parse this format and may silently return NULL — which
# would drop every event from the windowed aggregation. The `_nyc` variants
# are plain `yyyy-MM-dd HH:mm:ss` strings that parse cleanly under
# session.timeZone=America/New_York.
#
# These names are the cross-block contract. If Block 3's emitted field names
# change, this schema breaks the join silently (from_json ignores unknown
# fields and produces nulls for missing ones). Keep in sync.
ENRICHED_WEATHER_SCHEMA = T.StructType([
    T.StructField("pickup_datetime_nyc", T.TimestampType(), nullable=False),
    T.StructField("dropoff_datetime_nyc", T.TimestampType(), nullable=True),
    T.StructField("PULocationID", T.IntegerType(), nullable=True),
    T.StructField("DOLocationID", T.IntegerType(), nullable=True),
    T.StructField("trip_miles", T.DoubleType(), nullable=True),
    T.StructField("trip_time", T.DoubleType(), nullable=True),  # SECONDS
    T.StructField("weather_temperature_c", T.DoubleType(), nullable=True),
    T.StructField("weather_precipitation_mm", T.DoubleType(), nullable=True),
])

# Conversion: TLC trip_time is in seconds; hourly_demand.avg_trip_time is in
# minutes (per data model PDF). Divide by this in aggregate_window.
SECONDS_PER_MINUTE = 60.0


# ===========================================================================
# Pure transforms — testable in isolation on static DataFrames
# ===========================================================================

def aggregate_window(events: DataFrame) -> DataFrame:
    """15-min tumbling aggregation per zone.

    Interpretation (i): single (window, zone) row carrying both pickup_count
    (trips picked up in this zone+window) and dropoff_count (trips dropped off
    in this zone+window — possibly a disjoint set of trips). Two parallel
    aggregations on the same window grain, joined by (window, zone).

    Streaming watermark discipline:
        Pickup-side branch watermarks pickup_datetime; dropoff-side branch
        watermarks dropoff_datetime. Both BEFORE their respective groupBy.
        Spark's stream-stream join planner requires the watermark to be on
        the column the windowing key is derived from. Single upstream
        watermark on pickup_datetime would only watermark the pickup branch,
        and the full_outer join would be rejected at planning time.
        (Tested with batch DataFrames in unit tests; the watermark logic is
        a streaming-only concern that static-DataFrame tests cannot catch.)

    Field-name translation across block boundary:
        Block 3 input:               Block 4 output:
          PULocationID         →       zone_id (pickup-side groupBy key)
          DOLocationID         →       zone_id (dropoff-side groupBy key)
          trip_time (seconds)  →       avg_trip_time (minutes; ÷60)
          weather_temperature_c →      avg_temperature
          weather_precipitation_mm →   precipitation_mm

    Output schema:
        time_bucket TIMESTAMP   -- window start, inclusive [start, start+15min)
        zone_id INT
        pickup_count INT
        dropoff_count INT
        avg_trip_miles DOUBLE
        avg_trip_time DOUBLE     -- minutes (converted from seconds)
        avg_temperature DOUBLE   -- nullable (null if no weather observed)
        precipitation_mm DOUBLE  -- averaged across trips in window, nullable
    """
    # Pickup-side branch: watermark, filter, then aggregate.
    # Watermark on the source event-time column (pickup_datetime) — Spark
    # propagates this watermark through the groupBy to the derived
    # time_bucket column automatically for stream-stream inner joins.
    pickup_branch = (
        events
        .filter(F.col("pickup_datetime").isNotNull() & F.col("PULocationID").isNotNull())
        .withWatermark("pickup_datetime", WATERMARK_DELAY)
    )
    pickup_agg = (
        pickup_branch.groupBy(
            F.window(F.col("pickup_datetime"), WINDOW_DURATION).alias("w"),
            F.col("PULocationID").alias("zone_id"),
        )
        .agg(
            F.count(F.lit(1)).alias("pickup_count"),
            F.avg("trip_miles").alias("avg_trip_miles"),
            F.avg(F.col("trip_time") / F.lit(SECONDS_PER_MINUTE)).alias("avg_trip_time"),
            F.avg("weather_temperature_c").alias("avg_temperature"),
            F.avg("weather_precipitation_mm").alias("precipitation_mm"),
        )
        .select(
            F.col("w.start").alias("time_bucket"),
            F.col("zone_id"),
            F.col("pickup_count").cast("int").alias("pickup_count"),
            F.col("avg_trip_miles"),
            F.col("avg_trip_time"),
            F.col("avg_temperature"),
            F.col("precipitation_mm"),
        )
    )

    # Dropoff-side branch: separately watermarked on dropoff_datetime.
    # Count only — other metrics are pickup-side. A trip's pickup zone and
    # dropoff zone are usually different, so this groupBy intentionally
    # produces a different (window, zone) key set than pickup_agg.
    dropoff_branch = (
        events
        .filter(F.col("dropoff_datetime").isNotNull() & F.col("DOLocationID").isNotNull())
        .withWatermark("dropoff_datetime", WATERMARK_DELAY)
    )
    dropoff_agg = (
        dropoff_branch.groupBy(
            F.window(F.col("dropoff_datetime"), WINDOW_DURATION).alias("w"),
            F.col("DOLocationID").alias("zone_id"),
        )
        .agg(F.count(F.lit(1)).alias("dropoff_count"))
        .select(
            F.col("w.start").alias("time_bucket"),
            F.col("zone_id"),
            F.col("dropoff_count").cast("int").alias("dropoff_count"),
        )
    )

    # INNER join (NOT full_outer): only emit (window, zone) rows where BOTH
    # pickups AND dropoffs occurred in the same 15-min window.
    #
    # Tradeoff: pure-origin zones (airport departures: trips pick up at LGA
    # but drop off elsewhere) and pure-destination zones (residential 3am
    # dropoffs: trips end here but don't originate) are excluded from
    # demand-per-zone output for that window. For our thesis analysis this
    # is acceptable — the dominant signal in NYC ride-hailing is mixed-flow
    # zones (Manhattan, dense Brooklyn) which always show both directions.
    #
    # Why not full_outer: Spark's stream-stream full_outer join requires a
    # watermark on the JOIN KEY (the derived time_bucket column), not just
    # on the upstream event-time columns. Re-applying withWatermark on
    # time_bucket after groupBy resets the watermark to 0 in Spark 3.5
    # (verified empirically — state grew without bound, watermark stuck at
    # epoch). Inner join uses only the upstream watermarks and works.
    #
    # Future work: switch to two independent output topics
    # (demand-per-zone-pickups, demand-per-zone-dropoffs) and merge at the
    # Postgres sink. Cleaner semantics, no stream-stream join needed.
    joined = (
        pickup_agg.join(dropoff_agg, on=["time_bucket", "zone_id"], how="inner")
    )

    return joined.select(
        "time_bucket", "zone_id",
        "pickup_count", "dropoff_count",
        "avg_trip_miles", "avg_trip_time",
        "avg_temperature", "precipitation_mm",
    )


def attach_baseline(demand: DataFrame, baseline: DataFrame) -> DataFrame:
    """Broadcast left-join demand with the per-zone, per-hour-of-week baseline.

    Derives hour_of_week from time_bucket in America/New_York:
        dow_monday_zero = (dayofweek + 5) % 7    # Spark dow: 1=Sun..7=Sat
        hour_of_week    = dow_monday_zero * 24 + hour
    Same convention as scripts/build_baseline.py.

    Computes:
        demand_baseline = baseline_mean_pickups * 0.25  (hourly → 15-min)
        ratio           = pickup_count / demand_baseline
                          NULL when demand_baseline = 0 (mathematically undefined)

    Left-join semantics: rows with no matching baseline (e.g. zone_id outside
    1..263) pass through with null baseline_mean_pickups, demand_baseline, ratio.
    """
    # NOTE: spark.sql.session.timeZone is set to America/New_York by _session.py,
    # so dayofweek/hour on the (UTC-instant) time_bucket are evaluated in NYC time.
    dow_monday_zero = (F.dayofweek("time_bucket") + F.lit(5)) % F.lit(7)
    hour_of_week_expr = (dow_monday_zero * F.lit(24) + F.hour("time_bucket")).cast("int")

    demand_keyed = demand.withColumn("hour_of_week", hour_of_week_expr)

    joined = demand_keyed.join(
        F.broadcast(baseline),
        on=["zone_id", "hour_of_week"],
        how="left",
    )

    # demand_baseline = baseline * 0.25 (preserves null when baseline is null)
    demand_baseline_col = (
        F.col("baseline_mean_pickups") * F.lit(HOURLY_TO_WINDOW_SCALE)
    )

    # ratio = pickup_count / demand_baseline, but NULL when baseline is 0
    # (mathematically undefined). when() defaults to null on no-match, which
    # is exactly what we want.
    ratio_col = F.when(
        F.col("baseline_mean_pickups").isNotNull()
        & (F.col("baseline_mean_pickups") > F.lit(0.0)),
        F.col("pickup_count") / demand_baseline_col,
    )  # else: NULL (both for missing baseline and zero baseline)

    return (
        joined
        .withColumn("demand_baseline", demand_baseline_col)
        .withColumn("ratio", ratio_col)
        .drop("hour_of_week")
    )


def extract_hotspots(enriched_demand: DataFrame) -> DataFrame:
    """Filter to alert-worthy rows and assign severity.

    Rules:
        baseline_mean_pickups >= 1.0   (suppress low-activity zones; islands etc.)
        AND ratio >= 2.0               (genuine demand spike)

    Severity:
        2.0 <= ratio < 3.0 → "warning"
        ratio >= 3.0       → "critical"

    Output schema matches hotspot_alerts table (and ALERT_PAYLOAD_FIELDS):
        detected_at TIMESTAMP   -- = time_bucket (window start)
        zone_id INT
        demand_current INT      -- = pickup_count
        demand_baseline DOUBLE  -- 15-min equivalent of baseline_mean_pickups
        ratio DOUBLE
        severity STRING         -- "warning" | "critical"
    """
    severity_col = F.when(
        F.col("ratio") >= F.lit(SEVERITY_CRITICAL_RATIO), F.lit("critical")
    ).otherwise(F.lit("warning"))

    return (
        enriched_demand
        .filter(
            (F.col("baseline_mean_pickups") >= F.lit(BASELINE_HOTSPOT_FLOOR))
            & (F.col("ratio") >= F.lit(SEVERITY_WARNING_RATIO))
        )
        .select(
            F.col("time_bucket").alias("detected_at"),
            F.col("zone_id"),
            F.col("pickup_count").alias("demand_current"),
            F.col("demand_baseline"),
            F.col("ratio"),
            severity_col.alias("severity"),
        )
    )


def to_demand_payload(demand: DataFrame) -> DataFrame:
    """Serialize demand-per-zone rows for Kafka. Key=zone_id, value=JSON.

    ignoreNullFields=false: avg_temperature/precipitation_mm appear as explicit
    nulls in the JSON object rather than being dropped, so downstream consumers
    see a consistent schema.
    """
    payload_struct = F.struct(*[F.col(c) for c in DEMAND_PAYLOAD_FIELDS])
    return demand.select(
        F.col("zone_id").cast("string").alias("key"),
        F.to_json(payload_struct, {"ignoreNullFields": "false"}).alias("value"),
    )


def to_alert_payload(alerts: DataFrame) -> DataFrame:
    """Serialize hotspot-alerts rows for Kafka. Key=zone_id, value=JSON."""
    payload_struct = F.struct(*[F.col(c) for c in ALERT_PAYLOAD_FIELDS])
    return alerts.select(
        F.col("zone_id").cast("string").alias("key"),
        F.to_json(payload_struct, {"ignoreNullFields": "false"}).alias("value"),
    )


# ===========================================================================
# Streaming setup
# ===========================================================================

def load_baseline(spark: SparkSession, csv_path: str) -> DataFrame:
    """Load the broadcast baseline lookup CSV into a small DataFrame.

    The CSV has 44,184 rows (263 zones × 168 hours-of-week). Well under any
    broadcast threshold, but we cache + materialize so the broadcast happens
    once at job start rather than on every micro-batch.
    """
    schema = T.StructType([
        T.StructField("zone_id", T.IntegerType(), nullable=False),
        T.StructField("hour_of_week", T.IntegerType(), nullable=False),
        T.StructField("baseline_mean_pickups", T.DoubleType(), nullable=False),
    ])
    df = (
        spark.read.option("header", "true")
        .schema(schema)
        .csv(csv_path)
    )
    df = df.cache()
    df.count()  # force materialization
    return df


def read_enriched_weather_stream(spark: SparkSession) -> DataFrame:
    """Subscribe to ride-events-enriched-weather and parse JSON into typed columns.

    Reads the `_nyc` timestamp variants (plain yyyy-MM-dd HH:mm:ss strings)
    and aliases them to pickup_datetime / dropoff_datetime so downstream
    code (aggregate_window) is unchanged. See ENRICHED_WEATHER_SCHEMA
    docstring for why we don't parse the ISO-with-offset variant.

    Does NOT attach a watermark here. Pickup-side and dropoff-side aggregations
    each need their own watermark on their respective event-time column
    (pickup_datetime vs dropoff_datetime); attaching a single watermark here
    would only watermark one side and the streaming planner would reject the
    full_outer join. See aggregate_window for the per-branch watermark setup.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.from_json(
            F.col("value").cast("string"),
            ENRICHED_WEATHER_SCHEMA,
        ).alias("e"))
        .select(
            # Alias the _nyc variants to the names downstream code expects.
            F.col("e.pickup_datetime_nyc").alias("pickup_datetime"),
            F.col("e.dropoff_datetime_nyc").alias("dropoff_datetime"),
            F.col("e.PULocationID").alias("PULocationID"),
            F.col("e.DOLocationID").alias("DOLocationID"),
            F.col("e.trip_miles").alias("trip_miles"),
            F.col("e.trip_time").alias("trip_time"),
            F.col("e.weather_temperature_c").alias("weather_temperature_c"),
            F.col("e.weather_precipitation_mm").alias("weather_precipitation_mm"),
        )
    )
    return parsed


def main() -> int:
    spark = get_session(app_name="aggregation")
    spark.sparkContext.setLogLevel("WARN")

    baseline_path = os.environ.get("BASELINE_CSV", DEFAULT_BASELINE_CSV)
    if not Path(baseline_path).exists():
        raise FileNotFoundError(
            f"Baseline CSV not found: {baseline_path}. "
            f"Run scripts/build_baseline.py first."
        )
    baseline = load_baseline(spark, baseline_path)

    events = read_enriched_weather_stream(spark)

    # Build the demand pipeline (lazy — no execution until streaming queries start)
    demand = aggregate_window(events)
    enriched_demand = attach_baseline(demand, baseline)
    alerts = extract_hotspots(enriched_demand)

    # Two output sinks, two streaming queries, two checkpoints
    demand_query = (
        to_demand_payload(
            enriched_demand.select(*DEMAND_PAYLOAD_FIELDS)
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("topic", DEMAND_TOPIC)
        .option("checkpointLocation", checkpoint_dir("aggregation_demand"))
        .outputMode("append")
        .start()
    )

    alert_query = (
        to_alert_payload(alerts)
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("topic", HOTSPOT_TOPIC)
        .option("checkpointLocation", checkpoint_dir("aggregation_alerts"))
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())