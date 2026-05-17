"""
Tests for spark/pg_sink.py (Phase 3 Block 6).

Three tiers (mirrors test_network.py / test_aggregation.py):

  Static checks (instant, no Spark):
    - Script parses, schema constants present
    - Topic-to-table mapping matches design
    - Conflict keys match natural identity columns
    - Table column lists match DDL (sql/create_sink_tables.sql)
    - Upsert SQL strings have ON CONFLICT clauses with correct keys
    - network_flows upsert has SQL window functions for in/out degree

  Unit tests (~30s, local SparkSession, no Kafka, no Postgres):
    - parse_trip_event: maps PULocationID/DOLocationID → pu_zone_id/do_zone_id
    - parse_trip_event: converts trip_time seconds → trip_time_min
    - parse_trip_event: drops rows with null PK fields
    - parse_hourly_demand: passthrough mapping
    - parse_hotspot_alert: passthrough mapping
    - parse_network_flow: 4-field emission, drops null keys

  Slow integration (~3 min, requires full cluster + Postgres):
    - TODO contract: produce to all 4 topics, assert Postgres rows present

Run unit only:
    pytest tests/test_pg_sink.py -v -m "not slow"
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession, functions as F

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_DIR = REPO_ROOT / "spark"
sys.path.insert(0, str(SPARK_DIR))

import pg_sink as sink  # noqa: E402


# ===========================================================================
# Tier 1: Static checks
# ===========================================================================

class TestStatic:

    def test_script_parses(self):
        import ast
        ast.parse((SPARK_DIR / "pg_sink.py").read_text())

    def test_topic_table_mapping(self):
        """Block 6 design: 4 source topics → 4 sink tables."""
        mapping = {t: tab for (t, tab, _) in sink.TOPIC_TABLE_MAPPING}
        assert mapping["ride-events-enriched"] == "trip_events"
        assert mapping["demand-per-zone"] == "hourly_demand"
        assert mapping["hotspot-alerts"] == "hotspot_alerts"
        assert mapping["network-flow-updates"] == "network_flows"

    def test_trip_events_reads_from_block_2_not_block_3(self):
        """Design decision: trip_events comes from ride-events-enriched
        (Block 2), NOT ride-events-enriched-weather (Block 3). Block 3's
        output is a superset; trip_events schema has no weather columns.
        See BLOCK_6_STATUS.md."""
        sources = {t for (t, _, _) in sink.TOPIC_TABLE_MAPPING}
        assert "ride-events-enriched" in sources
        assert "ride-events-enriched-weather" not in sources

    def test_conflict_keys_match_natural_identity(self):
        """Upsert keys are the natural identity columns per table."""
        assert sink.CONFLICT_KEYS["trip_events"] == (
            "pickup_datetime", "pu_zone_id", "do_zone_id", "dropoff_datetime"
        )
        assert sink.CONFLICT_KEYS["hourly_demand"] == ("time_bucket", "zone_id")
        assert sink.CONFLICT_KEYS["hotspot_alerts"] == ("detected_at", "zone_id")
        assert sink.CONFLICT_KEYS["network_flows"] == (
            "time_window", "origin_zone_id", "dest_zone_id"
        )

    def test_table_columns_match_ddl(self):
        """Column lists in TABLE_COLUMNS must match the DDL in
        sql/create_sink_tables.sql. id BIGSERIAL excluded (DB assigns)."""
        expected = {
            "trip_events": {
                "pickup_datetime", "dropoff_datetime",
                "pu_zone_id", "do_zone_id",
                "trip_miles", "trip_time_min",
                "hour_of_day", "day_of_week",
                "is_weekend", "is_rush_hour",
            },
            "hourly_demand": {
                "time_bucket", "zone_id",
                "pickup_count", "dropoff_count",
                "avg_trip_miles", "avg_trip_time",
                "avg_temperature", "precipitation_mm",
            },
            "hotspot_alerts": {
                "detected_at", "zone_id",
                "demand_current", "demand_baseline",
                "ratio", "severity",
            },
            "network_flows": {
                "time_window", "origin_zone_id", "dest_zone_id",
                "trip_count",
            },
        }
        for table, cols in expected.items():
            assert set(sink.TABLE_COLUMNS[table]) == cols, (
                f"TABLE_COLUMNS[{table!r}] mismatch with DDL")

    def test_upsert_sql_has_on_conflict(self):
        """Every upsert must use ON CONFLICT (...) DO UPDATE."""
        for table in ("trip_events", "hourly_demand", "hotspot_alerts"):
            sql_str = sink.build_upsert_sql(table)
            assert "ON CONFLICT" in sql_str
            assert "DO UPDATE" in sql_str
            for k in sink.CONFLICT_KEYS[table]:
                assert k in sql_str, f"conflict key {k!r} missing in {table} SQL"

    def test_network_flows_upsert_uses_window_functions(self):
        """network_flows special-case: in_degree / out_degree computed via
        SQL window functions at INSERT time. See BLOCK_5_STATUS.md and
        BLOCK_6_STATUS.md for the rationale (Spark Structured Streaming
        cannot compute these; SQL on the materialized table can)."""
        sql_str = sink.build_upsert_sql("network_flows")
        # Must compute degrees via OVER PARTITION BY
        assert "OVER" in sql_str
        assert "PARTITION BY" in sql_str
        # In-degree partitions by dest, out-degree partitions by origin
        assert "PARTITION BY time_window, dest_zone_id" in sql_str
        assert "PARTITION BY time_window, origin_zone_id" in sql_str
        assert "AS in_degree" in sql_str
        assert "AS out_degree" in sql_str
        # And of course it's still an upsert
        assert "ON CONFLICT" in sql_str
        assert "time_window" in sql_str
        assert "origin_zone_id" in sql_str
        assert "dest_zone_id" in sql_str

    def test_trigger_intervals_per_pipeline(self):
        """Each topic's trigger matches its source cadence — uniform trigger
        would waste wakeups on slower sources."""
        triggers = {table: trig for (_, table, trig) in sink.TOPIC_TABLE_MAPPING}
        assert triggers["trip_events"] == "30 seconds"
        assert triggers["hourly_demand"] == "30 seconds"
        assert triggers["hotspot_alerts"] == "30 seconds"
        # Block 5 emits every 30 min; matching that
        assert triggers["network_flows"] == "30 minutes"


# ===========================================================================
# Tier 2: Unit tests with Spark — synthetic Kafka-shape input
# ===========================================================================

@pytest.fixture(scope="module")
def spark() -> SparkSession:
    s = (
        SparkSession.builder
        .appName("test_pg_sink")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _kafka_row(value_dict: dict) -> Row:
    """Build a fake Kafka row with `value` as JSON bytes."""
    return Row(
        key=b"k",
        value=json.dumps(value_dict).encode("utf-8"),
        topic="test",
        partition=0,
        offset=0,
    )


def _make_raw_kafka_df(spark, rows):
    """Build a DataFrame matching Spark's Kafka source schema (key=binary,
    value=binary, ...). Only `value` is read by the parse functions."""
    return spark.createDataFrame(
        rows,
        schema="key BINARY, value BINARY, topic STRING, partition INT, offset LONG",
    )


# ---------------------------------------------------------------------------
# parse_trip_event
# ---------------------------------------------------------------------------

class TestParseTripEvent:

    def test_maps_zone_ids_and_converts_trip_time_to_minutes(self, spark):
        raw = _make_raw_kafka_df(spark, [
            _kafka_row({
                "pickup_datetime_nyc":  "2024-07-15 14:30:00",
                "dropoff_datetime_nyc": "2024-07-15 14:42:00",
                "PULocationID": 230,
                "DOLocationID": 138,
                "trip_miles": 2.5,
                "trip_time": 720.0,  # 12 minutes in seconds
                "hour_of_day": 14,
                "day_of_week": 0,  # Monday
                "is_weekend": False,
                "is_rush_hour": False,
            }),
        ])
        result = sink.parse_trip_event(raw).collect()
        assert len(result) == 1
        r = result[0]
        # Zone IDs renamed
        assert r["pu_zone_id"] == 230
        assert r["do_zone_id"] == 138
        # Trip time converted from seconds to minutes
        assert r["trip_time_min"] == 12.0
        # Other fields passed through
        assert r["trip_miles"] == 2.5
        assert r["hour_of_day"] == 14
        assert r["day_of_week"] == 0
        assert r["is_weekend"] is False
        assert r["is_rush_hour"] is False

    def test_drops_rows_with_null_required_fields(self, spark):
        """Rows missing pickup_datetime, dropoff_datetime, PULocationID, or
        DOLocationID are dropped (PK violation otherwise)."""
        raw = _make_raw_kafka_df(spark, [
            # Valid row
            _kafka_row({
                "pickup_datetime_nyc": "2024-07-15 14:30:00",
                "dropoff_datetime_nyc": "2024-07-15 14:42:00",
                "PULocationID": 230, "DOLocationID": 138,
                "trip_miles": 2.5, "trip_time": 720.0,
                "hour_of_day": 14, "day_of_week": 0,
                "is_weekend": False, "is_rush_hour": False,
            }),
            # Null pickup
            _kafka_row({
                "pickup_datetime_nyc": None,
                "dropoff_datetime_nyc": "2024-07-15 14:42:00",
                "PULocationID": 230, "DOLocationID": 138,
                "trip_miles": 2.5, "trip_time": 720.0,
                "hour_of_day": 14, "day_of_week": 0,
                "is_weekend": False, "is_rush_hour": False,
            }),
            # Null PU
            _kafka_row({
                "pickup_datetime_nyc": "2024-07-15 14:30:00",
                "dropoff_datetime_nyc": "2024-07-15 14:42:00",
                "PULocationID": None, "DOLocationID": 138,
                "trip_miles": 2.5, "trip_time": 720.0,
                "hour_of_day": 14, "day_of_week": 0,
                "is_weekend": False, "is_rush_hour": False,
            }),
        ])
        result = sink.parse_trip_event(raw).collect()
        assert len(result) == 1
        assert result[0]["pu_zone_id"] == 230


# ---------------------------------------------------------------------------
# parse_hourly_demand
# ---------------------------------------------------------------------------

class TestParseHourlyDemand:

    def test_passthrough_mapping(self, spark):
        """Block 4 already emits with hourly_demand-aligned field names."""
        raw = _make_raw_kafka_df(spark, [
            _kafka_row({
                "time_bucket": "2024-07-15 14:15:00",
                "zone_id": 138,
                "pickup_count": 42,
                "dropoff_count": 38,
                "avg_trip_miles": 4.5,
                "avg_trip_time": 18.2,
                "avg_temperature": 17.4,
                "precipitation_mm": 0.0,
            }),
        ])
        result = sink.parse_hourly_demand(raw).collect()
        assert len(result) == 1
        r = result[0]
        assert r["zone_id"] == 138
        assert r["pickup_count"] == 42
        assert r["dropoff_count"] == 38
        assert r["avg_temperature"] == 17.4

    def test_null_weather_preserved(self, spark):
        """avg_temperature and precipitation_mm can legitimately be null
        (no weather observation for that window). The parse must allow this."""
        raw = _make_raw_kafka_df(spark, [
            _kafka_row({
                "time_bucket": "2024-07-15 14:15:00",
                "zone_id": 138,
                "pickup_count": 42,
                "dropoff_count": 38,
                "avg_trip_miles": 4.5,
                "avg_trip_time": 18.2,
                "avg_temperature": None,
                "precipitation_mm": None,
            }),
        ])
        result = sink.parse_hourly_demand(raw).collect()
        assert len(result) == 1
        assert result[0]["avg_temperature"] is None
        assert result[0]["precipitation_mm"] is None


# ---------------------------------------------------------------------------
# parse_hotspot_alert
# ---------------------------------------------------------------------------

class TestParseHotspotAlert:

    def test_passthrough_mapping(self, spark):
        raw = _make_raw_kafka_df(spark, [
            _kafka_row({
                "detected_at": "2024-07-15 14:15:00",
                "zone_id": 138,
                "demand_current": 120,
                "demand_baseline": 59.59,
                "ratio": 2.014,
                "severity": "warning",
            }),
        ])
        result = sink.parse_hotspot_alert(raw).collect()
        assert len(result) == 1
        r = result[0]
        assert r["zone_id"] == 138
        assert r["demand_current"] == 120
        assert r["severity"] == "warning"

    def test_null_ratio_preserved(self, spark):
        """Block 4 emits null ratio for zero-baseline zones (mathematically
        undefined). Parse must preserve this."""
        raw = _make_raw_kafka_df(spark, [
            _kafka_row({
                "detected_at": "2024-07-15 03:00:00",
                "zone_id": 199,
                "demand_current": 5,
                "demand_baseline": 0.0,
                "ratio": None,
                "severity": "critical",
            }),
        ])
        result = sink.parse_hotspot_alert(raw).collect()
        assert len(result) == 1
        assert result[0]["ratio"] is None


# ---------------------------------------------------------------------------
# parse_network_flow
# ---------------------------------------------------------------------------

class TestParseNetworkFlow:

    def test_four_field_emission(self, spark):
        """Streaming side emits only 4 fields. Degrees come from SQL at INSERT."""
        raw = _make_raw_kafka_df(spark, [
            _kafka_row({
                "time_window": "2024-07-15 14:00:00",
                "origin_zone_id": 230,
                "dest_zone_id": 138,
                "trip_count": 5,
            }),
        ])
        result = sink.parse_network_flow(raw).collect()
        assert len(result) == 1
        r = result[0]
        assert r["origin_zone_id"] == 230
        assert r["dest_zone_id"] == 138
        assert r["trip_count"] == 5
        # Verify no degree columns leak through
        assert "in_degree" not in r.asDict()
        assert "out_degree" not in r.asDict()

    def test_drops_null_keys(self, spark):
        raw = _make_raw_kafka_df(spark, [
            _kafka_row({
                "time_window": "2024-07-15 14:00:00",
                "origin_zone_id": 230, "dest_zone_id": 138, "trip_count": 5,
            }),
            _kafka_row({
                "time_window": None,
                "origin_zone_id": 230, "dest_zone_id": 138, "trip_count": 5,
            }),
            _kafka_row({
                "time_window": "2024-07-15 14:00:00",
                "origin_zone_id": None, "dest_zone_id": 138, "trip_count": 5,
            }),
        ])
        result = sink.parse_network_flow(raw).collect()
        assert len(result) == 1


# ===========================================================================
# Tier 3: Slow integration — placeholder
# ===========================================================================

@pytest.mark.slow
class TestSlowIntegration:
    """End-to-end test: produce to all 4 topics, run pg_sink, assert Postgres.

    TODO contract — what this should verify when filled in:

    Setup:
      - Spin up pg_sink.py via spark-submit subprocess
      - Override trigger intervals to ~10s for fast finalization
      - Unique consumer-group and checkpoint dir per run (uuid suffix)

    Producers (synthetic events to each topic):
      - ride-events-enriched: 3 trip events
      - demand-per-zone: 2 hourly_demand rows
      - hotspot-alerts: 1 alert
      - network-flow-updates: 2 OD pair flows in same window

    Assertions on Postgres:
      - trip_events has 3 rows
      - hourly_demand has 2 rows
      - hotspot_alerts has 1 row
      - network_flows has 2 rows, with in_degree/out_degree correctly
        computed via the SQL window functions
      - Re-running producers + sink: row counts UNCHANGED (idempotency)
    """

    def test_all_four_tables_populated(self):
        pytest.skip("TODO: implement after pg_sink.py is functional")