"""
Tests for spark/network.py (Phase 3 Block 5).

Three tiers (mirrors test_aggregation.py / test_enrichment.py):

  Static checks (instant, no Spark):
    - Script parses, schema constants present, topic name correct
    - Output schema matches network_flows Postgres table
    - Window/trigger/watermark constants are the agreed values

  Unit tests (~30s, local SparkSession, no Kafka, synthetic input DFs):
    - aggregate_od_flows: 1-hour tumbling, groups by (window, origin, dest)
    - aggregate_od_flows: window boundary inclusivity ([start, end))
    - aggregate_od_flows: trip_count is correct per OD pair
    - attach_degrees: out_degree per origin = sum(trip_count) over same window
    - attach_degrees: in_degree per dest = sum(trip_count) over same window
    - attach_degrees: zones appearing as both origin and dest get both metrics
    - attach_degrees: single-trip edge case (in=out=1)
    - to_kafka_payload: key=origin_zone_id, value parses as JSON
    - to_kafka_payload: preserves null fields (ignoreNullFields=false)

  Slow integration (~3 min, requires full cluster):
    - Submit network.py as subprocess
    - Produce synthetic enriched-weather events directly to source topic
    - Verify network-flow-updates has rows with all 6 fields

Run unit only (default):
    pytest tests/test_network.py -v -m "not slow"

Run slow integration:
    pytest tests/test_network.py -v -m slow
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import Row, SparkSession, functions as F

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_DIR = REPO_ROOT / "spark"
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SPARK_DIR))
sys.path.insert(0, str(SCRIPTS_DIR))

# --- Static check imports (no SparkSession needed) ---
import network as net  # noqa: E402

NYC = ZoneInfo("America/New_York")


def _project_window_key(df, ts_col: str = "time_window"):
    """Add a 'window_key' STRING column 'YYYY-MM-DD HH:mm' formatted in NYC tz.

    Same trick as test_aggregation.py: Spark's .collect() returns Python
    datetimes in JVM default tz (Asia/Ho_Chi_Minh on this VM), not in
    session.timeZone. Project to a string via F.date_format BEFORE collect
    so the value is tz-agnostic.
    """
    return df.withColumn(
        "window_key",
        F.date_format(F.col(ts_col), "yyyy-MM-dd HH:mm"),
    )


def _window_key_str(y, mo, d, h, mi) -> str:
    """Build a window key string matching _project_window_key output."""
    return f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}"


# ===========================================================================
# Tier 1: Static checks — instant, no Spark
# ===========================================================================

class TestStatic:
    """Fail fast on schema drift or constant drift before paying JVM cost."""

    def test_script_parses(self):
        import ast
        ast.parse((SPARK_DIR / "network.py").read_text())

    def test_topic_names(self):
        assert net.SOURCE_TOPIC == "ride-events-enriched-weather"
        assert net.SINK_TOPIC == "network-flow-updates"

    def test_window_size_is_one_hour(self):
        """Spec: 1-hour tumbling windows on pickup_datetime."""
        assert net.WINDOW_DURATION == "1 hour"

    def test_trigger_is_thirty_minutes(self):
        """Spec page 9: trigger every 30 minutes. Matches the natural cadence
        of 1-hour windows + 30-min watermark."""
        assert net.TRIGGER_INTERVAL == "30 minutes"

    def test_watermark_is_thirty_minutes(self):
        """Pipeline-wide convention: every stateful Spark job uses 30 min."""
        assert net.WATERMARK_DELAY == "30 minutes"

    def test_payload_schema_matches_network_flows_table(self):
        """JSON payload keys are the streaming-side subset of network_flows
        columns. in_degree/out_degree are computed by the PG Sink via SQL
        window functions; betweenness is computed by Phase 4 Colab.
        See BLOCK_5_STATUS.md for the scope-split rationale."""
        expected = {
            "time_window", "origin_zone_id", "dest_zone_id", "trip_count",
        }
        assert set(net.PAYLOAD_FIELDS) == expected


# ===========================================================================
# Tier 2: Unit tests with Spark — synthetic enriched-weather input DFs
# ===========================================================================

@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Module-scoped SparkSession. JVM cold-start paid once per file."""
    s = (
        SparkSession.builder
        .appName("test_network")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _make_enriched_weather_df(spark, rows):
    """Build a synthetic ride-events-enriched-weather DataFrame.

    `rows`: list of dicts with keys pickup_iso, PULocationID, DOLocationID.
    Other fields from Block 3's schema are present but irrelevant to network
    aggregation; we fill in valid placeholders.

    Timestamp construction discipline: ISO STRING + F.to_timestamp inside
    Spark. See test_aggregation.py for the JVM-tz rationale we learned the
    hard way in Block 4.
    """
    expanded = []
    for r in rows:
        expanded.append(Row(
            pickup_iso=r["pickup_iso"],
            dropoff_iso=r.get("dropoff_iso", r["pickup_iso"]),
            PULocationID=int(r["PULocationID"]),
            DOLocationID=int(r["DOLocationID"]),
            trip_miles=float(r.get("trip_miles", 1.0)),
            trip_time=float(r.get("trip_time_seconds", 600.0)),
            weather_temperature_c=None,
            weather_precipitation_mm=None,
        ))
    df_str = spark.createDataFrame(
        expanded,
        schema=("pickup_iso STRING, dropoff_iso STRING, "
                "PULocationID INT, DOLocationID INT, "
                "trip_miles DOUBLE, trip_time DOUBLE, "
                "weather_temperature_c DOUBLE, weather_precipitation_mm DOUBLE"),
    )
    return df_str.select(
        F.to_timestamp("pickup_iso").alias("pickup_datetime"),
        F.to_timestamp("dropoff_iso").alias("dropoff_datetime"),
        "PULocationID", "DOLocationID",
        "trip_miles", "trip_time",
        "weather_temperature_c", "weather_precipitation_mm",
    )


# ---------------------------------------------------------------------------
# aggregate_od_flows: groupBy (window, origin, dest), count
# ---------------------------------------------------------------------------

class TestAggregateOdFlows:

    def test_groups_by_one_hour_window_and_od_pair(self, spark):
        """Three trips:
          (14:30 230→138), (14:45 230→138) — same hour, same OD → count=2
          (14:50 230→132) — same hour, different dest → separate row, count=1
          (15:10 230→138) — next hour, same OD → separate row, count=1
        """
        rows = [
            {"pickup_iso": "2024-07-15T14:30:00", "PULocationID": 230, "DOLocationID": 138},
            {"pickup_iso": "2024-07-15T14:45:00", "PULocationID": 230, "DOLocationID": 138},
            {"pickup_iso": "2024-07-15T14:50:00", "PULocationID": 230, "DOLocationID": 132},
            {"pickup_iso": "2024-07-15T15:10:00", "PULocationID": 230, "DOLocationID": 138},
        ]
        events = _make_enriched_weather_df(spark, rows)
        result = _project_window_key(net.aggregate_od_flows(events)).collect()
        by_key = {
            (r["window_key"], r["origin_zone_id"], r["dest_zone_id"]): r["trip_count"]
            for r in result
        }
        # 1-hour windows: 14:00–15:00 and 15:00–16:00
        win_14 = _window_key_str(2024, 7, 15, 14, 0)
        win_15 = _window_key_str(2024, 7, 15, 15, 0)
        assert by_key[(win_14, 230, 138)] == 2
        assert by_key[(win_14, 230, 132)] == 1
        assert by_key[(win_15, 230, 138)] == 1
        assert len(by_key) == 3

    def test_window_lower_bound_inclusive_upper_exclusive(self, spark):
        """time_window=14:00 covers [14:00:00.000, 15:00:00.000).
        A trip at exactly 15:00:00 belongs to the NEXT window."""
        rows = [
            {"pickup_iso": "2024-07-15T14:00:00", "PULocationID": 230, "DOLocationID": 138},
            {"pickup_iso": "2024-07-15T15:00:00", "PULocationID": 230, "DOLocationID": 138},
        ]
        events = _make_enriched_weather_df(spark, rows)
        result = {
            (r["window_key"], r["origin_zone_id"], r["dest_zone_id"]): r["trip_count"]
            for r in _project_window_key(net.aggregate_od_flows(events)).collect()
        }
        win_14 = _window_key_str(2024, 7, 15, 14, 0)
        win_15 = _window_key_str(2024, 7, 15, 15, 0)
        assert result[(win_14, 230, 138)] == 1
        assert result[(win_15, 230, 138)] == 1
        assert (win_14, 230, 138) in result
        assert (win_15, 230, 138) in result

    def test_self_loops_allowed(self, spark):
        """A trip with PU == DO (same zone, e.g. very short ride within a
        large zone like LGA) is a legitimate OD pair. Not filtered out."""
        rows = [
            {"pickup_iso": "2024-07-15T14:30:00", "PULocationID": 138, "DOLocationID": 138},
            {"pickup_iso": "2024-07-15T14:45:00", "PULocationID": 138, "DOLocationID": 138},
        ]
        events = _make_enriched_weather_df(spark, rows)
        result = _project_window_key(net.aggregate_od_flows(events)).collect()
        assert len(result) == 1
        assert result[0]["origin_zone_id"] == 138
        assert result[0]["dest_zone_id"] == 138
        assert result[0]["trip_count"] == 2

    def test_null_zones_filtered(self, spark):
        """Trips with NULL PULocationID or DOLocationID can't contribute to
        an OD pair and should be dropped, not crash the aggregation.

        This is the same data-quality contract enforced in Block 4."""
        # Build directly with nulls — _make_enriched_weather_df doesn't
        # support nullable IDs by design, so build a custom DF
        df_str = spark.createDataFrame([
            Row(pickup_iso="2024-07-15T14:30:00",
                dropoff_iso="2024-07-15T14:42:00",
                PULocationID=230, DOLocationID=138,
                trip_miles=2.0, trip_time=720.0,
                weather_temperature_c=None, weather_precipitation_mm=None),
            Row(pickup_iso="2024-07-15T14:35:00",
                dropoff_iso="2024-07-15T14:50:00",
                PULocationID=None, DOLocationID=138,  # null origin
                trip_miles=2.0, trip_time=720.0,
                weather_temperature_c=None, weather_precipitation_mm=None),
            Row(pickup_iso="2024-07-15T14:40:00",
                dropoff_iso="2024-07-15T14:55:00",
                PULocationID=230, DOLocationID=None,  # null dest
                trip_miles=2.0, trip_time=720.0,
                weather_temperature_c=None, weather_precipitation_mm=None),
        ], schema=("pickup_iso STRING, dropoff_iso STRING, "
                   "PULocationID INT, DOLocationID INT, "
                   "trip_miles DOUBLE, trip_time DOUBLE, "
                   "weather_temperature_c DOUBLE, weather_precipitation_mm DOUBLE"))
        events = df_str.select(
            F.to_timestamp("pickup_iso").alias("pickup_datetime"),
            F.to_timestamp("dropoff_iso").alias("dropoff_datetime"),
            "PULocationID", "DOLocationID",
            "trip_miles", "trip_time",
            "weather_temperature_c", "weather_precipitation_mm",
        )
        result = net.aggregate_od_flows(events).collect()
        # Only the first row had both PU and DO non-null
        assert len(result) == 1
        assert result[0]["trip_count"] == 1


# ---------------------------------------------------------------------------
# Payload serialization
# ---------------------------------------------------------------------------

class TestKafkaPayload:

    def _flows_df(self, spark, rows):
        """Build a one-or-more-row aggregate_od_flows-output DataFrame.

        `rows`: list of dicts with window_iso, origin_zone_id, dest_zone_id,
        trip_count.
        """
        df_str = spark.createDataFrame(
            [Row(window_iso=r["window_iso"],
                 origin_zone_id=int(r["origin_zone_id"]),
                 dest_zone_id=int(r["dest_zone_id"]),
                 trip_count=int(r["trip_count"]))
             for r in rows],
            schema=("window_iso STRING, origin_zone_id INT, "
                    "dest_zone_id INT, trip_count INT"),
        )
        return df_str.select(
            F.to_timestamp("window_iso").alias("time_window"),
            "origin_zone_id", "dest_zone_id", "trip_count",
        )

    def test_payload_keyed_by_origin_zone_id(self, spark):
        """Kafka key is origin_zone_id (as string) so all flows from the same
        origin land on the same partition — better locality for downstream
        consumers filtering on origin."""
        df = self._flows_df(spark, [
            {"window_iso": "2024-07-15T14:00:00",
             "origin_zone_id": 230, "dest_zone_id": 138, "trip_count": 5},
        ])
        payload = net.to_kafka_payload(df).collect()[0]
        assert payload["key"] == "230"
        body = json.loads(payload["value"])
        assert body["origin_zone_id"] == 230
        assert body["dest_zone_id"] == 138
        assert body["trip_count"] == 5

    def test_payload_has_all_four_fields(self, spark):
        """Schema check: all PAYLOAD_FIELDS must appear in the JSON output."""
        df = self._flows_df(spark, [
            {"window_iso": "2024-07-15T14:00:00",
             "origin_zone_id": 230, "dest_zone_id": 138, "trip_count": 5},
        ])
        body = json.loads(net.to_kafka_payload(df).collect()[0]["value"])
        for field in net.PAYLOAD_FIELDS:
            assert field in body, f"missing field {field!r} in payload"


# ===========================================================================
# Tier 3: Slow integration — placeholder, fill in after smoke verification
# ===========================================================================

@pytest.mark.slow
class TestSlowIntegration:
    """End-to-end test producing directly to ride-events-enriched-weather
    (skipping Block 2/3 jobs entirely). Same pattern as Block 4's deferred
    slow-integration test.

    TODO contract — what this should verify when filled in:

    Setup:
      - Spin up network.py via spark-submit subprocess
      - Use unique consumer-group and checkpoint dir per run (uuid suffix)
      - Override TRIGGER_INTERVAL and WATERMARK_DELAY via env vars to ~10s
        for fast window finalization in the test (production stays at
        30 min / 30 min)

    Producer (synthetic enriched-weather events):
      - 20 events spread across 3 OD pairs in one window:
          (230→138) × 10 trips
          (230→132) × 5 trips
          (138→230) × 5 trips
      - All within a single 1-hour window

    Assertions on network-flow-updates:
      - 3 emitted rows (one per OD pair)
      - trip_counts match: 10, 5, 5
      - PAYLOAD_FIELDS appear in each JSON body
    """

    def test_network_flow_updates_emitted(self):
        pytest.skip("TODO: implement after network.py is functional")