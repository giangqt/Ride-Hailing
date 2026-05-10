"""
Tests for spark/aggregation.py (Phase 3 Block 4).

Three tiers (mirrors test_enrichment.py / test_enrichment_weather.py):

  Static checks (instant, no Spark):
    - Script parses, schema constants present, topic names correct
    - Output schemas match Postgres tables (hourly_demand, hotspot_alerts)
    - Severity thresholds and baseline floor are the agreed values

  Unit tests (~30s, local SparkSession, no Kafka, synthetic input DFs):
    - aggregate_window: 15-min tumbling, counts and avgs per zone
    - aggregate_window: window boundary inclusivity (start inclusive, end exclusive)
    - attach_baseline: broadcast left-join on (zone_id, hour_of_week), 15-min scaling
    - attach_baseline: hour_of_week derived from window-start in NYC time
    - extract_hotspots: filters baseline >= 1.0 AND ratio >= 2.0
    - extract_hotspots: severity 2.0<=ratio<3.0 = warning, ratio>=3.0 = critical
    - extract_hotspots: zero-baseline zones (Liberty Island) emit no alerts
    - to_demand_payload / to_alert_payload: ignoreNullFields=false, key=zone_id

  Slow integration (~3 min, requires full cluster):
    - Submit aggregation.py as subprocess
    - Produce synthetic enriched-weather events directly to
      ride-events-enriched-weather (skip upstream Block 2/3 jobs)
    - Verify demand-per-zone has a row per (window, zone_id) with right counts
    - Verify hotspot-alerts only fires when ratio >= 2.0

Run unit only (default):
    pytest tests/test_aggregation.py -v -m "not slow"

Run slow integration:
    pytest tests/test_aggregation.py -v -m slow
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
import aggregation as agg  # noqa: E402

NYC = ZoneInfo("America/New_York")


def _project_bucket_key(df, ts_col: str = "time_bucket"):
    """Add a 'bucket_key' string column 'YYYY-MM-DD HH:mm' formatted in NYC tz.

    Spark's .collect() deserializes TIMESTAMP columns to Python datetimes in
    the JVM default timezone (NOT session.timeZone) — a known PySpark quirk.
    On VMs where the JVM tz isn't NYC (e.g. Asia/Ho_Chi_Minh on this dev box),
    raw collected datetimes are shifted by the (JVM_tz - NYC) delta.

    Workaround: format the timestamp into a string INSIDE Spark using
    date_format under session.timeZone=NYC, then collect the string. Strings
    don't get tz-converted by Py4J. The collected value is the wall-clock we
    asserted on, regardless of JVM tz.
    """
    return df.withColumn(
        "bucket_key",
        F.date_format(F.col(ts_col), "yyyy-MM-dd HH:mm"),
    )


def _bucket_key_str(y, mo, d, h, mi) -> str:
    """Build a 'YYYY-MM-DD HH:mm' string for assertion against bucket_key."""
    return f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}"


# ===========================================================================
# Tier 1: Static checks — instant, no Spark
# ===========================================================================

class TestStatic:
    """Fail fast on schema drift or constant drift before paying JVM cost."""

    def test_script_parses(self):
        import ast
        ast.parse((SPARK_DIR / "aggregation.py").read_text())

    def test_topic_names(self):
        assert agg.SOURCE_TOPIC == "ride-events-enriched-weather"
        assert agg.DEMAND_TOPIC == "demand-per-zone"
        assert agg.HOTSPOT_TOPIC == "hotspot-alerts"

    def test_window_size_is_fifteen_minutes(self):
        assert agg.WINDOW_DURATION == "15 minutes"

    def test_watermark_is_thirty_minutes(self):
        """Pipeline-wide convention: every stateful Spark job uses 30-min watermark."""
        assert agg.WATERMARK_DELAY == "30 minutes"

    def test_baseline_suppression_floor(self):
        """Below this floor, a zone is too low-activity to trigger hotspots reliably."""
        assert agg.BASELINE_HOTSPOT_FLOOR == 1.0

    def test_severity_thresholds(self):
        assert agg.SEVERITY_WARNING_RATIO == 2.0
        assert agg.SEVERITY_CRITICAL_RATIO == 3.0

    def test_demand_payload_schema_matches_hourly_demand_table(self):
        """demand-per-zone JSON keys must map cleanly to hourly_demand columns."""
        expected = {
            "time_bucket", "zone_id", "pickup_count", "dropoff_count",
            "avg_trip_miles", "avg_trip_time", "avg_temperature",
            "precipitation_mm",
        }
        assert set(agg.DEMAND_PAYLOAD_FIELDS) == expected

    def test_alert_payload_schema_matches_hotspot_alerts_table(self):
        """hotspot-alerts JSON keys must map cleanly to hotspot_alerts columns."""
        expected = {
            "detected_at", "zone_id", "demand_current", "demand_baseline",
            "ratio", "severity",
        }
        assert set(agg.ALERT_PAYLOAD_FIELDS) == expected


# ===========================================================================
# Tier 2: Unit tests with Spark — synthetic enriched-weather input DFs
# ===========================================================================

@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Module-scoped SparkSession. JVM cold-start paid once per file."""
    s = (
        SparkSession.builder
        .appName("test_aggregation")
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

    `rows` is a list of dicts with keys matching Block 3's output schema:
      pickup_iso, dropoff_iso, PULocationID, DOLocationID,
      trip_miles, trip_time_seconds,
      weather_temperature_c, weather_precipitation_mm.
    NOTE: trip_time is in SECONDS (matching TLC source); aggregate_window
    converts to minutes for avg_trip_time.

    Timestamp construction discipline:
        Pass timestamps as ISO STRINGS, not Python datetime objects, then
        cast to TIMESTAMP via F.to_timestamp under session.timeZone=NYC.
        Why: Py4J converts Python datetime → JVM Timestamp using the JVM's
        default timezone (e.g. Asia/Ho_Chi_Minh on this VM, UTC+7), which
        contaminates the stored UTC instant before session.timeZone can
        intervene. F.to_timestamp on a string parses the wall-clock under
        session.timeZone (NYC) — which is what we want.
    """
    expanded = []
    for r in rows:
        do_loc = r.get("DOLocationID", r["PULocationID"])
        expanded.append(Row(
            pickup_iso=r["pickup_iso"],
            dropoff_iso=r["dropoff_iso"],
            PULocationID=int(r["PULocationID"]),
            DOLocationID=int(do_loc),
            trip_miles=float(r["trip_miles"]),
            trip_time=float(r["trip_time_seconds"]),
            weather_temperature_c=(None if r.get("weather_temperature_c") is None
                                   else float(r["weather_temperature_c"])),
            weather_precipitation_mm=(None if r.get("weather_precipitation_mm") is None
                                      else float(r["weather_precipitation_mm"])),
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


def _make_baseline_df(spark, rows):
    """Build a synthetic baseline lookup DataFrame.

    `rows` is a list of (zone_id, hour_of_week, baseline_mean_pickups) tuples.
    """
    return spark.createDataFrame(
        rows,
        schema="zone_id INT, hour_of_week INT, baseline_mean_pickups DOUBLE",
    )


# ---------------------------------------------------------------------------
# aggregate_window: 15-min tumbling, counts and avgs per zone
# ---------------------------------------------------------------------------

class TestAggregateWindow:
    """The pure-aggregation transform. No baseline join, no hotspot logic."""

    def test_groups_by_15min_window_and_zone(self, spark):
        # Two trips in same 15-min window, same zone → one row, count=2
        # One trip in next window, same zone → second row, count=1
        # One trip in same window as first pair, different zone → third row, count=1
        rows = [
            {"pickup_iso": "2024-07-15T14:30:00", "dropoff_iso": "2024-07-15T14:42:00",
             "PULocationID": 230, "trip_miles": 2.0, "trip_time_seconds": 720.0,
             "weather_temperature_c": 25.0, "weather_precipitation_mm": 0.0},
            {"pickup_iso": "2024-07-15T14:38:00", "dropoff_iso": "2024-07-15T14:55:00",
             "PULocationID": 230, "trip_miles": 4.0, "trip_time_seconds": 1020.0,
             "weather_temperature_c": 25.0, "weather_precipitation_mm": 0.0},
            {"pickup_iso": "2024-07-15T14:46:00", "dropoff_iso": "2024-07-15T15:05:00",
             "PULocationID": 230, "trip_miles": 6.0, "trip_time_seconds": 1140.0,
             "weather_temperature_c": 25.0, "weather_precipitation_mm": 0.0},
            {"pickup_iso": "2024-07-15T14:32:00", "dropoff_iso": "2024-07-15T14:50:00",
             "PULocationID": 138, "trip_miles": 8.0, "trip_time_seconds": 1080.0,
             "weather_temperature_c": 25.0, "weather_precipitation_mm": 0.0},
        ]
        events = _make_enriched_weather_df(spark, rows)
        # Project time_bucket to a NYC-formatted string column in Spark BEFORE
        # collecting. See _project_bucket_key docstring for the JVM-tz rationale.
        result = _project_bucket_key(agg.aggregate_window(events)).collect()

        # Index by (bucket_key_string, zone_id) for tz-safe stable lookups.
        # Filter to rows where pickup_count > 0 to focus on pickup activity.
        pickup_rows = [r for r in result if r["pickup_count"] > 0]
        by_key = {(r["bucket_key"], r["zone_id"]): r for r in pickup_rows}
        assert len(by_key) == 3, f"expected 3 (window, zone) pickup groups, got {len(by_key)}"

        # Window 14:30–14:45, zone 230: trips at 14:30 and 14:38 → count=2
        bucket_1430 = _bucket_key_str(2024, 7, 15, 14, 30)
        bucket_1445 = _bucket_key_str(2024, 7, 15, 14, 45)
        assert by_key[(bucket_1430, 230)]["pickup_count"] == 2
        assert by_key[(bucket_1430, 230)]["avg_trip_miles"] == pytest.approx(3.0)
        # Window 14:45–15:00, zone 230: trip at 14:46 → count=1
        assert by_key[(bucket_1445, 230)]["pickup_count"] == 1
        # Window 14:30–14:45, zone 138: trip at 14:32 → count=1
        assert by_key[(bucket_1430, 138)]["pickup_count"] == 1

    def test_window_lower_bound_inclusive_upper_exclusive(self, spark):
        """time_bucket=14:30 means [14:30:00.000, 14:45:00.000).
        A trip at exactly 14:45:00 belongs to the NEXT window."""
        rows = [
            # Exactly at the lower bound → belongs to 14:30 window
            {"pickup_iso": "2024-07-15T14:30:00", "dropoff_iso": "2024-07-15T14:40:00",
             "PULocationID": 230, "trip_miles": 1.0, "trip_time_seconds": 600.0,
             "weather_temperature_c": 20.0, "weather_precipitation_mm": 0.0},
            # Exactly at the upper bound → belongs to 14:45 window, NOT 14:30
            {"pickup_iso": "2024-07-15T14:45:00", "dropoff_iso": "2024-07-15T14:55:00",
             "PULocationID": 230, "trip_miles": 2.0, "trip_time_seconds": 600.0,
             "weather_temperature_c": 20.0, "weather_precipitation_mm": 0.0},
        ]
        events = _make_enriched_weather_df(spark, rows)
        # Filter to pickup-side rows (pickup_count > 0) to avoid noise from
        # the dropoff-side aggregation, which puts trips into different windows.
        result = {(r["bucket_key"], r["zone_id"]): r["pickup_count"]
                  for r in _project_bucket_key(agg.aggregate_window(events)).collect()
                  if r["pickup_count"] > 0}
        bucket_1430 = _bucket_key_str(2024, 7, 15, 14, 30)
        bucket_1445 = _bucket_key_str(2024, 7, 15, 14, 45)
        assert result[(bucket_1430, 230)] == 1
        assert result[(bucket_1445, 230)] == 1

    def test_dropoff_count_uses_dropoff_window_and_dropoff_zone(self, spark):
        """Interpretation (i): a trip with PU in window A, zone X and DO in
        window B, zone Y produces:
          (A, X) → pickup_count=1, dropoff_count=0
          (B, Y) → pickup_count=0, dropoff_count=1
        Two separate aggregation rows, joined nowhere because the keys differ.
        """
        rows = [
            # Trip A: pickup in window 14:30, zone 230; dropoff in window 14:45, zone 138
            {"pickup_iso": "2024-07-15T14:35:00", "dropoff_iso": "2024-07-15T14:50:00",
             "PULocationID": 230, "DOLocationID": 138,
             "trip_miles": 5.0, "trip_time_seconds": 900.0,
             "weather_temperature_c": 25.0, "weather_precipitation_mm": 0.0},
        ]
        events = _make_enriched_weather_df(spark, rows)
        result = {(r["bucket_key"], r["zone_id"]): r
                  for r in _project_bucket_key(agg.aggregate_window(events)).collect()}

        bucket_1430 = _bucket_key_str(2024, 7, 15, 14, 30)
        bucket_1445 = _bucket_key_str(2024, 7, 15, 14, 45)

        # (14:30, zone 230): the pickup row
        assert result[(bucket_1430, 230)]["pickup_count"] == 1
        assert result[(bucket_1430, 230)]["dropoff_count"] == 0

        # (14:45, zone 138): the dropoff row, in a different window AND different zone
        assert result[(bucket_1445, 138)]["pickup_count"] == 0
        assert result[(bucket_1445, 138)]["dropoff_count"] == 1

    def test_trip_time_converted_seconds_to_minutes(self, spark):
        """Block 3 emits trip_time in SECONDS (inherited from TLC source).
        hourly_demand.avg_trip_time is in MINUTES per the data model.
        A 600-second trip must show as 10.0 in avg_trip_time."""
        rows = [
            {"pickup_iso": "2024-07-15T14:30:00", "dropoff_iso": "2024-07-15T14:40:00",
             "PULocationID": 230, "trip_miles": 1.0, "trip_time_seconds": 600.0,
             "weather_temperature_c": 20.0, "weather_precipitation_mm": 0.0},
        ]
        events = _make_enriched_weather_df(spark, rows)
        result = [r for r in agg.aggregate_window(events).collect()
                  if r["pickup_count"] > 0][0]
        assert result["avg_trip_time"] == pytest.approx(10.0), (
            "trip_time=600 seconds must convert to avg_trip_time=10 minutes"
        )

    def test_avg_temperature_and_precipitation_aggregated(self, spark):
        """Weather columns are averaged across trips in the window.
        Both temperature and precipitation are observation values (instantaneous
        rates), not per-event quantities — so averaging is correct for both."""
        rows = [
            {"pickup_iso": "2024-07-15T14:30:00", "dropoff_iso": "2024-07-15T14:40:00",
             "PULocationID": 230, "trip_miles": 1.0, "trip_time_seconds": 600.0,
             "weather_temperature_c": 20.0, "weather_precipitation_mm": 0.5},
            {"pickup_iso": "2024-07-15T14:35:00", "dropoff_iso": "2024-07-15T14:45:00",
             "PULocationID": 230, "trip_miles": 1.0, "trip_time_seconds": 600.0,
             "weather_temperature_c": 30.0, "weather_precipitation_mm": 1.5},
        ]
        events = _make_enriched_weather_df(spark, rows)
        r = [row for row in agg.aggregate_window(events).collect()
             if row["pickup_count"] > 0][0]
        assert r["avg_temperature"] == pytest.approx(25.0)
        assert r["precipitation_mm"] == pytest.approx(1.0)

    def test_null_weather_handled_without_crash(self, spark):
        """Trips with NULL weather (Block 3 didn't find a matching observation)
        must not break the aggregation. avg over (val, NULL) = val; all-NULL → NULL."""
        rows = [
            {"pickup_iso": "2024-07-15T14:30:00", "dropoff_iso": "2024-07-15T14:40:00",
             "PULocationID": 230, "trip_miles": 1.0, "trip_time_seconds": 600.0,
             "weather_temperature_c": 20.0, "weather_precipitation_mm": 0.0},
            {"pickup_iso": "2024-07-15T14:35:00", "dropoff_iso": "2024-07-15T14:45:00",
             "PULocationID": 230, "trip_miles": 1.0, "trip_time_seconds": 600.0,
             "weather_temperature_c": None, "weather_precipitation_mm": None},
        ]
        events = _make_enriched_weather_df(spark, rows)
        r = [row for row in agg.aggregate_window(events).collect()
             if row["pickup_count"] > 0][0]
        assert r["avg_temperature"] == pytest.approx(20.0)
        assert r["pickup_count"] == 2  # NULL weather doesn't drop the trip


# ---------------------------------------------------------------------------
# attach_baseline: broadcast left-join + hour_of_week derivation + 15-min scale
# ---------------------------------------------------------------------------

class TestAttachBaseline:

    def _demand_row(self, spark, time_bucket_iso, zone_id, pickup_count):
        """Build a one-row demand DF (output of aggregate_window) for testing
        attach_baseline in isolation.

        Timestamp goes via STRING + F.to_timestamp to avoid Py4J's JVM-default-tz
        contamination. See _make_enriched_weather_df docstring for rationale.
        """
        df_str = spark.createDataFrame(
            [Row(time_bucket_iso=time_bucket_iso,
                 zone_id=int(zone_id),
                 pickup_count=int(pickup_count),
                 dropoff_count=0,
                 avg_trip_miles=0.0, avg_trip_time=0.0,
                 avg_temperature=None, precipitation_mm=None)],
            schema=("time_bucket_iso STRING, zone_id INT, "
                    "pickup_count INT, dropoff_count INT, "
                    "avg_trip_miles DOUBLE, avg_trip_time DOUBLE, "
                    "avg_temperature DOUBLE, precipitation_mm DOUBLE"),
        )
        return df_str.select(
            F.to_timestamp("time_bucket_iso").alias("time_bucket"),
            "zone_id", "pickup_count", "dropoff_count",
            "avg_trip_miles", "avg_trip_time",
            "avg_temperature", "precipitation_mm",
        )

    def test_hour_of_week_derived_from_window_start_in_nyc(self, spark):
        """2024-07-15 14:30 NYC = Monday, hour_of_day=14
        → hour_of_week = 0*24 + 14 = 14"""
        demand = self._demand_row(spark, "2024-07-15T14:30:00", 230, 100)
        # Baseline keyed at hour_of_week=14, zone=230, baseline=200 pickups/hr
        baseline = _make_baseline_df(spark, [(230, 14, 200.0)])
        result = agg.attach_baseline(demand, baseline).collect()[0]
        # demand_baseline = 200 * 0.25 = 50 (15-min equivalent)
        assert result["demand_baseline"] == pytest.approx(50.0)
        # ratio = 100 / 50 = 2.0
        assert result["ratio"] == pytest.approx(2.0)

    def test_sunday_hour_of_week_is_six_times_24_plus_hour(self, spark):
        """2024-07-21 was a Sunday. Monday=0 convention → Sunday=6.
        Sunday 09:00 → hour_of_week = 6*24 + 9 = 153."""
        demand = self._demand_row(spark, "2024-07-21T09:00:00", 230, 50)
        baseline = _make_baseline_df(spark, [(230, 153, 80.0)])
        result = agg.attach_baseline(demand, baseline).collect()[0]
        # 50 / (80 * 0.25) = 50 / 20 = 2.5
        assert result["ratio"] == pytest.approx(2.5)

    def test_left_join_preserves_demand_when_baseline_missing(self, spark):
        """If a (zone, hour_of_week) is somehow missing from the broadcast
        baseline, the demand row must still pass through (with null baseline).
        In practice build_baseline.py emits all 263×168 cells, so this is
        defense-in-depth, not an expected case."""
        demand = self._demand_row(spark, "2024-07-15T14:30:00", 999, 10)
        baseline = _make_baseline_df(spark, [(230, 14, 200.0)])
        result = agg.attach_baseline(demand, baseline).collect()
        assert len(result) == 1, "demand row must not be dropped on missing baseline"
        assert result[0]["zone_id"] == 999
        assert result[0]["pickup_count"] == 10
        assert result[0]["demand_baseline"] is None

    def test_zero_baseline_yields_null_ratio(self, spark):
        """Liberty Island has baseline=0 across all 168 hours. ratio = current/0
        is mathematically undefined and MUST serialize as null — not 0, which
        would imply we computed a real value. demand_baseline can be 0 (it's
        the actual baseline) but ratio must be null."""
        demand = self._demand_row(spark, "2024-07-15T14:30:00", 103, 5)
        baseline = _make_baseline_df(spark, [(103, 14, 0.0)])
        result = agg.attach_baseline(demand, baseline).collect()[0]
        assert result["zone_id"] == 103
        assert result["pickup_count"] == 5
        assert result["ratio"] is None, (
            "ratio for zero-baseline zones must be NULL (undefined), not 0"
        )


# ---------------------------------------------------------------------------
# extract_hotspots: filter + severity assignment
# ---------------------------------------------------------------------------

class TestExtractHotspots:

    def _enriched_demand_df(self, spark, rows):
        """Build a one-or-more-row attach_baseline-output DataFrame.

        `rows` is list of (time_bucket_iso, zone_id, pickup_count, baseline,
                           demand_baseline, ratio).
        Explicit schema required: some columns can be None (avg_temperature,
        precipitation_mm, demand_baseline, ratio in zero-baseline cases),
        and Spark cannot infer types from None values alone.

        Timestamp via STRING + F.to_timestamp; see _make_enriched_weather_df
        docstring for the Py4J-default-tz rationale.
        """
        expanded = [
            Row(
                time_bucket_iso=r[0],
                zone_id=int(r[1]),
                pickup_count=int(r[2]),
                dropoff_count=0,
                avg_trip_miles=0.0,
                avg_trip_time=0.0,
                avg_temperature=None,
                precipitation_mm=None,
                baseline_mean_pickups=float(r[3]),
                demand_baseline=float(r[4]) if r[4] is not None else None,
                ratio=float(r[5]) if r[5] is not None else None,
            )
            for r in rows
        ]
        schema = (
            "time_bucket_iso STRING, zone_id INT, "
            "pickup_count INT, dropoff_count INT, "
            "avg_trip_miles DOUBLE, avg_trip_time DOUBLE, "
            "avg_temperature DOUBLE, precipitation_mm DOUBLE, "
            "baseline_mean_pickups DOUBLE, demand_baseline DOUBLE, ratio DOUBLE"
        )
        df_str = spark.createDataFrame(expanded, schema=schema)
        return df_str.select(
            F.to_timestamp("time_bucket_iso").alias("time_bucket"),
            "zone_id", "pickup_count", "dropoff_count",
            "avg_trip_miles", "avg_trip_time",
            "avg_temperature", "precipitation_mm",
            "baseline_mean_pickups", "demand_baseline", "ratio",
        )

    def test_no_alert_below_warning_threshold(self, spark):
        """ratio = 1.99 → no alert."""
        df = self._enriched_demand_df(spark, [
            ("2024-07-15T14:30:00", 230, 99, 200.0, 50.0, 1.98),
        ])
        alerts = agg.extract_hotspots(df).collect()
        assert alerts == []

    def test_warning_at_2x(self, spark):
        df = self._enriched_demand_df(spark, [
            ("2024-07-15T14:30:00", 230, 100, 200.0, 50.0, 2.0),
        ])
        alerts = agg.extract_hotspots(df).collect()
        assert len(alerts) == 1
        assert alerts[0]["severity"] == "warning"
        assert alerts[0]["ratio"] == pytest.approx(2.0)

    def test_warning_just_below_critical(self, spark):
        df = self._enriched_demand_df(spark, [
            ("2024-07-15T14:30:00", 230, 149, 200.0, 50.0, 2.98),
        ])
        alerts = agg.extract_hotspots(df).collect()
        assert alerts[0]["severity"] == "warning"

    def test_critical_at_3x(self, spark):
        df = self._enriched_demand_df(spark, [
            ("2024-07-15T14:30:00", 230, 150, 200.0, 50.0, 3.0),
        ])
        alerts = agg.extract_hotspots(df).collect()
        assert alerts[0]["severity"] == "critical"

    def test_critical_well_above(self, spark):
        df = self._enriched_demand_df(spark, [
            ("2024-07-15T14:30:00", 230, 500, 200.0, 50.0, 10.0),
        ])
        alerts = agg.extract_hotspots(df).collect()
        assert alerts[0]["severity"] == "critical"

    def test_low_baseline_zone_suppressed(self, spark):
        """Liberty Island (baseline=0.0): even with 100 pickups, no alert.
        Same for any zone with baseline_mean_pickups < 1.0."""
        df = self._enriched_demand_df(spark, [
            ("2024-07-15T14:30:00", 103, 100, 0.0, 0.0, None),
            ("2024-07-15T14:30:00", 199, 50, 0.5, 0.125, 400.0),  # very high ratio
        ])
        alerts = agg.extract_hotspots(df).collect()
        assert alerts == [], (
            "zones with baseline < 1.0 must be suppressed regardless of ratio"
        )

    def test_alert_payload_carries_correct_fields(self, spark):
        df = self._enriched_demand_df(spark, [
            ("2024-07-15T14:30:00", 230, 150, 200.0, 50.0, 3.0),
        ])
        # Project detected_at to a NYC-formatted string before collecting,
        # to bypass JVM-tz conversion in .collect(). See _project_bucket_key.
        alert = _project_bucket_key(
            agg.extract_hotspots(df), ts_col="detected_at"
        ).collect()[0]
        assert alert["bucket_key"] == _bucket_key_str(2024, 7, 15, 14, 30)
        assert alert["zone_id"] == 230
        assert alert["demand_current"] == 150
        assert alert["demand_baseline"] == pytest.approx(50.0)
        assert alert["ratio"] == pytest.approx(3.0)
        assert alert["severity"] == "critical"


# ---------------------------------------------------------------------------
# Payload serialization: JSON shape, key=zone_id, ignoreNullFields=false
# ---------------------------------------------------------------------------

class TestKafkaPayloads:

    def test_demand_payload_keyed_by_zone_id_and_parses_as_json(self, spark):
        df_str = spark.createDataFrame(
            [Row(time_bucket_iso="2024-07-15T14:30:00",
                 zone_id=230, pickup_count=100, dropoff_count=80,
                 avg_trip_miles=2.5, avg_trip_time=12.0,
                 avg_temperature=25.0, precipitation_mm=0.0)],
            schema=("time_bucket_iso STRING, zone_id INT, "
                    "pickup_count INT, dropoff_count INT, "
                    "avg_trip_miles DOUBLE, avg_trip_time DOUBLE, "
                    "avg_temperature DOUBLE, precipitation_mm DOUBLE"),
        )
        df = df_str.select(
            F.to_timestamp("time_bucket_iso").alias("time_bucket"),
            "zone_id", "pickup_count", "dropoff_count",
            "avg_trip_miles", "avg_trip_time",
            "avg_temperature", "precipitation_mm",
        )
        payload = agg.to_demand_payload(df).collect()[0]
        assert payload["key"] == "230"
        body = json.loads(payload["value"])
        assert body["zone_id"] == 230
        assert body["pickup_count"] == 100

    def test_demand_payload_preserves_null_weather_fields(self, spark):
        """ignoreNullFields=false: avg_temperature=None must serialize as
        explicit null, not be dropped from the JSON object."""
        df_str = spark.createDataFrame(
            [Row(time_bucket_iso="2024-07-15T14:30:00",
                 zone_id=230, pickup_count=100, dropoff_count=80,
                 avg_trip_miles=2.5, avg_trip_time=12.0,
                 avg_temperature=None, precipitation_mm=None)],
            schema=("time_bucket_iso STRING, zone_id INT, "
                    "pickup_count INT, dropoff_count INT, "
                    "avg_trip_miles DOUBLE, avg_trip_time DOUBLE, "
                    "avg_temperature DOUBLE, precipitation_mm DOUBLE"),
        )
        df = df_str.select(
            F.to_timestamp("time_bucket_iso").alias("time_bucket"),
            "zone_id", "pickup_count", "dropoff_count",
            "avg_trip_miles", "avg_trip_time",
            "avg_temperature", "precipitation_mm",
        )
        body = json.loads(agg.to_demand_payload(df).collect()[0]["value"])
        assert "avg_temperature" in body, "null fields must be preserved (ignoreNullFields=false)"
        assert body["avg_temperature"] is None
        assert body["precipitation_mm"] is None

    def test_alert_payload_keyed_by_zone_id_and_parses_as_json(self, spark):
        df_str = spark.createDataFrame(
            [Row(detected_at_iso="2024-07-15T14:30:00",
                 zone_id=230, demand_current=150, demand_baseline=50.0,
                 ratio=3.0, severity="critical")],
            schema=("detected_at_iso STRING, zone_id INT, "
                    "demand_current INT, demand_baseline DOUBLE, "
                    "ratio DOUBLE, severity STRING"),
        )
        df = df_str.select(
            F.to_timestamp("detected_at_iso").alias("detected_at"),
            "zone_id", "demand_current", "demand_baseline",
            "ratio", "severity",
        )
        payload = agg.to_alert_payload(df).collect()[0]
        assert payload["key"] == "230"
        body = json.loads(payload["value"])
        assert body["severity"] == "critical"
        assert body["ratio"] == 3.0


# ===========================================================================
# Tier 3: Slow integration — real Kafka, synthetic enriched-weather producer
# ===========================================================================

@pytest.mark.slow
class TestSlowIntegration:
    """End-to-end test producing directly to ride-events-enriched-weather
    (skipping Block 2/3 jobs entirely). Per the unit-testing directive:
    Block 4 tests must not require the upstream pipeline to be running.

    TODO contract — what this should verify when filled in:

    Setup:
      - Spin up aggregation.py via `spark-submit` as a subprocess (host mode,
        local[*] master). Pattern matches test_enrichment.py's slow tier.
      - Use unique consumer-group and checkpoint dir per run (uuid suffix) so
        re-running tests doesn't read stale offsets or corrupt state.
      - Build a small synthetic baseline CSV (~5 zones, hand-crafted) and
        point aggregation.py at it via env var; do NOT depend on the real
        44k-row baseline file under data/baseline/.

    Producer (synthetic enriched-weather events):
      - Use kafka-python KafkaProducer to write directly to
        ride-events-enriched-weather. Construct ~30 events spread across:
          (a) one zone with normal demand (count ~= baseline*0.25 → no alert)
          (b) one zone with 2.5x demand (→ warning)
          (c) one zone with 5x demand (→ critical)
          (d) Liberty Island (zone 103, baseline=0 in synthetic CSV) with
              high demand (→ no alert, suppression rule)
      - All events fall within a single 15-min window so we can deterministically
        check counts. Pickup_datetime values use a fixed UTC instant so the
        test isn't host-clock-dependent.

    Consumer (verify outputs):
      - Subscribe to demand-per-zone and hotspot-alerts with a fresh group_id.
      - Wait up to ~90s (15-min window + 30-min watermark would be too long for
        a test; for integration we override WATERMARK_DELAY to e.g. '5 seconds'
        via env var so the window finalizes promptly. Document this clearly —
        production uses 30 min, the override is a test-only convenience.)
      - Assertions on demand-per-zone:
          * One row per active (window, zone)
          * pickup_count matches the producer's per-zone count
          * Schema has all 8 DEMAND_PAYLOAD_FIELDS, including null weather where
            it was null in the input
      - Assertions on hotspot-alerts:
          * Exactly two alerts emitted (zones b and c)
          * No alert for zone a (below threshold) or zone d (suppressed)
          * Severity strings match the ratio bands

    Teardown:
      - Terminate the spark-submit subprocess
      - Delete the per-run checkpoint dir
      - Do NOT delete the test topics — Kafka has auto-create disabled in this
        cluster, so test topics must be pre-created in the test setup or
        reused across runs.

    Estimated runtime: ~3 min (mostly JVM cold-start and waiting for the first
    streaming micro-batch to fire).
    """

    def test_demand_per_zone_and_hotspot_alerts_emitted(self):
        pytest.skip("TODO: implement after aggregation.py is functional")