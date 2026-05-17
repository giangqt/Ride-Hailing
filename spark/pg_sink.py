"""
Phase 3 Block 6: Postgres sink.

Reads 4 Kafka topics → writes 4 Postgres tables via upsert.

Topic                          → Table              Cadence (matches source)
─────────────────────────────────────────────────────────────────────────
ride-events-enriched           → trip_events        30s trigger
demand-per-zone                → hourly_demand      30s trigger
hotspot-alerts                 → hotspot_alerts     30s trigger
network-flow-updates           → network_flows      30 min trigger

Idempotency: every write uses INSERT ... ON CONFLICT DO UPDATE keyed on
the natural identity columns. At-least-once Kafka + Spark retries can
cause duplicate emissions; the sink converges to the latest emitted value.

Output mode: 'update' is required because writes happen inside foreachBatch.
Append mode would be incompatible with the JDBC upsert pattern.

Architecture: foreachBatch + psycopg2.extras.execute_values, not Spark's
native JDBC sink, because native JDBC does not support ON CONFLICT.
df.collect() to driver is a thesis-scale simplification — production
would use foreachPartition for parallel upserts across executors.

Network flows: in_degree / out_degree are derived from trip_count at
INSERT time via SQL window functions:
    SUM(trip_count) OVER (PARTITION BY time_window, origin_zone_id) AS out_degree
    SUM(trip_count) OVER (PARTITION BY time_window, dest_zone_id)   AS in_degree
Spark Structured Streaming cannot compute these (NON_TIME_WINDOW_NOT_SUPPORTED_
IN_STREAMING); SQL on the materialized table works cleanly.

Run modes:
    Host:   python spark/pg_sink.py
    Docker: docker compose exec spark-master spark-submit \\
                --master spark://spark-master:7077 \\
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \\
                /app/spark/pg_sink.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from spark._session import (  # noqa: E402
    get_session,
    kafka_brokers,
    checkpoint_dir,
)


# ===========================================================================
# Constants — referenced by tests; do not change without updating contract
# ===========================================================================

# Topic ↔ table mapping. Order in this list is the order of pipeline setup.
TOPIC_TABLE_MAPPING = [
    ("ride-events-enriched",        "trip_events",     "30 seconds"),
    ("demand-per-zone",             "hourly_demand",   "30 seconds"),
    ("hotspot-alerts",              "hotspot_alerts",  "30 seconds"),
    ("network-flow-updates",        "network_flows",   "30 minutes"),
]

# Per-table conflict key columns (used in ON CONFLICT clause)
CONFLICT_KEYS = {
    "trip_events":      ("pickup_datetime", "pu_zone_id", "do_zone_id", "dropoff_datetime"),
    "hourly_demand":    ("time_bucket", "zone_id"),
    "hotspot_alerts":   ("detected_at", "zone_id"),
    "network_flows":    ("time_window", "origin_zone_id", "dest_zone_id"),
}

# Column lists for each table (excluding id BIGSERIAL — DB assigns it)
TABLE_COLUMNS = {
    "trip_events": (
        "pickup_datetime", "dropoff_datetime",
        "pu_zone_id", "do_zone_id",
        "trip_miles", "trip_time_min",
        "hour_of_day", "day_of_week",
        "is_weekend", "is_rush_hour",
    ),
    "hourly_demand": (
        "time_bucket", "zone_id",
        "pickup_count", "dropoff_count",
        "avg_trip_miles", "avg_trip_time",
        "avg_temperature", "precipitation_mm",
    ),
    "hotspot_alerts": (
        "detected_at", "zone_id",
        "demand_current", "demand_baseline",
        "ratio", "severity",
    ),
    "network_flows": (
        "time_window", "origin_zone_id", "dest_zone_id",
        "trip_count",
    ),
    # in_degree / out_degree are computed at INSERT via window functions
    # betweenness is left NULL (populated by Phase 4 Colab)
}


# ===========================================================================
# Source schemas — JSON shape of each Kafka topic
# ===========================================================================

TRIP_EVENT_SCHEMA = T.StructType([
    T.StructField("pickup_datetime_nyc",  T.TimestampType(), nullable=False),
    T.StructField("dropoff_datetime_nyc", T.TimestampType(), nullable=False),
    T.StructField("PULocationID",         T.IntegerType(),   nullable=False),
    T.StructField("DOLocationID",         T.IntegerType(),   nullable=False),
    T.StructField("trip_miles",           T.DoubleType(),    nullable=True),
    T.StructField("trip_time",            T.DoubleType(),    nullable=True),  # seconds
    T.StructField("hour_of_day",          T.IntegerType(),   nullable=True),
    T.StructField("day_of_week",          T.IntegerType(),   nullable=True),
    T.StructField("is_weekend",           T.BooleanType(),   nullable=True),
    T.StructField("is_rush_hour",         T.BooleanType(),   nullable=True),
])

HOURLY_DEMAND_SCHEMA = T.StructType([
    # time_bucket is read as STRING and converted via F.to_timestamp in
    # parse_hourly_demand. Block 4's aggregation.py emits ISO-with-offset
    # format (e.g. "2026-05-10T12:15:00.000-04:00") which Spark's from_json
    # silently returns NULL for when the field type is TimestampType.
    # See BLOCK_4_STATUS.md "lessons learned" for the full rationale.
    T.StructField("time_bucket",       T.StringType(),    nullable=False),
    T.StructField("zone_id",           T.IntegerType(),   nullable=False),
    T.StructField("pickup_count",      T.IntegerType(),   nullable=False),
    T.StructField("dropoff_count",     T.IntegerType(),   nullable=False),
    T.StructField("avg_trip_miles",    T.DoubleType(),    nullable=True),
    T.StructField("avg_trip_time",     T.DoubleType(),    nullable=True),
    T.StructField("avg_temperature",   T.DoubleType(),    nullable=True),
    T.StructField("precipitation_mm",  T.DoubleType(),    nullable=True),
])

HOTSPOT_ALERT_SCHEMA = T.StructType([
    # Same ISO-with-offset issue as time_bucket above.
    T.StructField("detected_at",     T.StringType(),    nullable=False),
    T.StructField("zone_id",         T.IntegerType(),   nullable=False),
    T.StructField("demand_current",  T.IntegerType(),   nullable=False),
    T.StructField("demand_baseline", T.DoubleType(),    nullable=False),
    T.StructField("ratio",           T.DoubleType(),    nullable=True),
    T.StructField("severity",        T.StringType(),    nullable=False),
])

NETWORK_FLOW_SCHEMA = T.StructType([
    # Same ISO-with-offset issue. Block 5's network.py emits the
    # window-start timestamp the same way.
    T.StructField("time_window",     T.StringType(),    nullable=False),
    T.StructField("origin_zone_id",  T.IntegerType(),   nullable=False),
    T.StructField("dest_zone_id",    T.IntegerType(),   nullable=False),
    T.StructField("trip_count",      T.IntegerType(),   nullable=False),
])


# ===========================================================================
# Pure parse transforms — testable on batch DataFrames
# ===========================================================================

def parse_trip_event(raw_kafka_df: DataFrame) -> DataFrame:
    """Parse JSON from ride-events-enriched into trip_events column shape.

    Reads pickup_datetime_nyc / dropoff_datetime_nyc (plain wall-clock variants
    that parse reliably; see BLOCK_4_STATUS.md). Maps PULocationID/DOLocationID
    to pu_zone_id/do_zone_id. Converts trip_time seconds → trip_time_min.
    """
    return (
        raw_kafka_df
        .select(F.from_json(F.col("value").cast("string"), TRIP_EVENT_SCHEMA).alias("e"))
        .filter(
            F.col("e.pickup_datetime_nyc").isNotNull()
            & F.col("e.dropoff_datetime_nyc").isNotNull()
            & F.col("e.PULocationID").isNotNull()
            & F.col("e.DOLocationID").isNotNull()
        )
        .select(
            F.col("e.pickup_datetime_nyc").alias("pickup_datetime"),
            F.col("e.dropoff_datetime_nyc").alias("dropoff_datetime"),
            F.col("e.PULocationID").alias("pu_zone_id"),
            F.col("e.DOLocationID").alias("do_zone_id"),
            F.col("e.trip_miles"),
            (F.col("e.trip_time") / F.lit(60.0)).alias("trip_time_min"),
            F.col("e.hour_of_day"),
            F.col("e.day_of_week"),
            F.col("e.is_weekend"),
            F.col("e.is_rush_hour"),
        )
    )


def parse_hourly_demand(raw_kafka_df: DataFrame) -> DataFrame:
    """Parse JSON from demand-per-zone. Block 4's emitted payload is already
    aligned with the hourly_demand column names — no remapping needed.

    Defensive timestamp handling: time_bucket arrives as ISO-with-offset
    string ("2026-05-10T12:15:00.000-04:00"). Read as StringType then convert
    via F.to_timestamp, which handles the offset cleanly (whereas from_json
    on TimestampType returns NULL silently for this format).
    """
    return (
        raw_kafka_df
        .select(F.from_json(F.col("value").cast("string"), HOURLY_DEMAND_SCHEMA).alias("e"))
        .filter(F.col("e.time_bucket").isNotNull() & F.col("e.zone_id").isNotNull())
        .select(
            F.to_timestamp(F.col("e.time_bucket")).alias("time_bucket"),
            F.col("e.zone_id"),
            F.col("e.pickup_count"),
            F.col("e.dropoff_count"),
            F.col("e.avg_trip_miles"),
            F.col("e.avg_trip_time"),
            F.col("e.avg_temperature"),
            F.col("e.precipitation_mm"),
        )
    )


def parse_hotspot_alert(raw_kafka_df: DataFrame) -> DataFrame:
    """Parse JSON from hotspot-alerts. Same ISO-with-offset timestamp
    defensive handling as parse_hourly_demand."""
    return (
        raw_kafka_df
        .select(F.from_json(F.col("value").cast("string"), HOTSPOT_ALERT_SCHEMA).alias("e"))
        .filter(F.col("e.detected_at").isNotNull() & F.col("e.zone_id").isNotNull())
        .select(
            F.to_timestamp(F.col("e.detected_at")).alias("detected_at"),
            F.col("e.zone_id"),
            F.col("e.demand_current"),
            F.col("e.demand_baseline"),
            F.col("e.ratio"),
            F.col("e.severity"),
        )
    )


def parse_network_flow(raw_kafka_df: DataFrame) -> DataFrame:
    """Parse JSON from network-flow-updates. Emits only the 4 streaming fields;
    in_degree/out_degree are computed in SQL at INSERT time.

    Same ISO-with-offset timestamp defensive handling as parse_hourly_demand."""
    return (
        raw_kafka_df
        .select(F.from_json(F.col("value").cast("string"), NETWORK_FLOW_SCHEMA).alias("e"))
        .filter(
            F.col("e.time_window").isNotNull()
            & F.col("e.origin_zone_id").isNotNull()
            & F.col("e.dest_zone_id").isNotNull()
        )
        .select(
            F.to_timestamp(F.col("e.time_window")).alias("time_window"),
            F.col("e.origin_zone_id"),
            F.col("e.dest_zone_id"),
            F.col("e.trip_count"),
        )
    )


# ===========================================================================
# Upsert SQL builders — pure functions, testable
# ===========================================================================

def build_upsert_sql(table: str) -> str:
    """Build the INSERT ... ON CONFLICT DO UPDATE SQL for `table`.

    Returns a parametrized SQL string for use with psycopg2.execute_values.
    network_flows is special-cased to derive in_degree/out_degree via window
    functions in the INSERT ... SELECT pattern.
    """
    if table == "network_flows":
        return _build_network_flows_upsert_sql()

    cols = TABLE_COLUMNS[table]
    keys = CONFLICT_KEYS[table]
    non_key_cols = [c for c in cols if c not in keys]

    col_list = ", ".join(cols)
    conflict_target = ", ".join(keys)
    if non_key_cols:
        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_key_cols)
        on_conflict = f"DO UPDATE SET {update_clause}"
    else:
        on_conflict = "DO NOTHING"

    return (
        f"INSERT INTO {table} ({col_list}) VALUES %s "
        f"ON CONFLICT ({conflict_target}) {on_conflict}"
    )


def _build_network_flows_upsert_sql() -> str:
    """Build the network_flows upsert with window-function degree computation.

    The trick: INSERT INTO ... SELECT FROM (VALUES ...) lets us run window
    functions over the batch's rows. The window partitions are scoped to the
    current batch only — degrees computed here cover trips IN THIS BATCH from
    the same window. For the full per-window degree across all batches, a
    later UPDATE pass (or a materialized view) would be needed. For thesis
    scale this is acceptable; Block 5's 30-min trigger means each batch
    contains a complete 1-hour window's worth of OD flows.
    """
    return """
        INSERT INTO network_flows (
            time_window, origin_zone_id, dest_zone_id, trip_count,
            in_degree, out_degree
        )
        SELECT
            time_window, origin_zone_id, dest_zone_id, trip_count,
            SUM(trip_count) OVER (PARTITION BY time_window, dest_zone_id)
                AS in_degree,
            SUM(trip_count) OVER (PARTITION BY time_window, origin_zone_id)
                AS out_degree
        FROM (VALUES %s) AS batch(time_window, origin_zone_id, dest_zone_id, trip_count)
        ON CONFLICT (time_window, origin_zone_id, dest_zone_id) DO UPDATE SET
            trip_count = EXCLUDED.trip_count,
            in_degree  = EXCLUDED.in_degree,
            out_degree = EXCLUDED.out_degree
    """.strip()


# ===========================================================================
# foreachBatch handlers — driver-side upsert via psycopg2
# ===========================================================================

def _pg_connect():
    """Single connection per foreachBatch call.

    Thesis-scale simplification: one connection per batch, opened and closed
    each time. Production would use a connection pool or foreachPartition
    with executor-side connections.
    """
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "rides"),
        user=os.environ.get("PG_USER", "rides"),
        password=os.environ.get("PG_PASSWORD", "rides"),
    )


def _make_upsert_handler(table: str):
    """Return a foreachBatch handler for `table`."""
    sql = build_upsert_sql(table)
    cols = (TABLE_COLUMNS[table]
            if table != "network_flows"
            else ("time_window", "origin_zone_id", "dest_zone_id", "trip_count"))

    def handler(batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            return
        values = [tuple(r[c] for c in cols) for r in rows]
        conn = _pg_connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, values, page_size=500)
        finally:
            conn.close()

    handler.__name__ = f"upsert_{table}"
    return handler


# ===========================================================================
# Stream setup
# ===========================================================================

def _make_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    """Subscribe to a topic from latest (production default; smoke test
    can override via env)."""
    starting_offsets = os.environ.get(
        f"PGSINK_STARTING_OFFSETS_{topic.replace('-', '_').upper()}",
        os.environ.get("PGSINK_STARTING_OFFSETS", "latest"),
    )
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


PARSE_FUNCS = {
    "trip_events":      parse_trip_event,
    "hourly_demand":    parse_hourly_demand,
    "hotspot_alerts":   parse_hotspot_alert,
    "network_flows":    parse_network_flow,
}


def main() -> int:
    spark = get_session(app_name="pg_sink")
    spark.sparkContext.setLogLevel("WARN")

    queries = []
    for topic, table, default_trigger in TOPIC_TABLE_MAPPING:
        # Smoke override: PGSINK_TRIGGER_INTERVAL_<TABLE_UPPER>
        trigger = os.environ.get(
            f"PGSINK_TRIGGER_INTERVAL_{table.upper()}",
            os.environ.get("PGSINK_TRIGGER_INTERVAL", default_trigger),
        )
        print(f"[pg_sink] {topic} → {table}  (trigger={trigger})", flush=True)

        raw = _make_kafka_stream(spark, topic)
        parsed = PARSE_FUNCS[table](raw)
        handler = _make_upsert_handler(table)

        q = (
            parsed
            .writeStream
            .foreachBatch(handler)
            .option("checkpointLocation", checkpoint_dir(f"pg_sink_{table}"))
            .outputMode("update")
            .trigger(processingTime=trigger)
            .start()
        )
        queries.append(q)

    print(f"[pg_sink] {len(queries)} streaming queries started", flush=True)
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())