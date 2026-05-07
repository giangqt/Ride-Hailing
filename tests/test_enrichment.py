"""
Tests for spark/enrichment.py.

Three tiers (same pattern as test_spark_hello.py):

  Static checks (instant):
    - Script parses, has main, schema is exactly 7 fields

  Unit tests (~30s, starts a small SparkSession):
    - Temporal features: hour, day_of_week, is_weekend, is_rush_hour
    - Rush-hour boundary cases (7:00, 9:59, 16:00, 19:59 weekday vs weekend)
    - Zone broadcast join attaches zone metadata correctly
    - LEFT join behavior: trips with unknown PULocationID still pass through
    - to_kafka_payload key is PULocationID and value is parseable JSON

  Slow integration (~2 min, marked):
    - Submit enrichment.py as subprocess, run producer in parallel,
      verify messages land on ride-events-enriched with correct shape

Run unit only (default):
    pytest tests/test_enrichment.py -v -m "not slow"

Run slow integration:
    pytest tests/test_enrichment.py -v -m slow
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_DIR = REPO_ROOT / "spark"
sys.path.insert(0, str(SPARK_DIR))

# --- Static check imports (no SparkSession needed) ---
import enrichment as enr  # noqa: E402


# ---------------------------------------------------------------------------
# Static checks — instant, no Spark
# ---------------------------------------------------------------------------

class TestStatic:

    def test_script_parses(self):
        import ast
        ast.parse((SPARK_DIR / "enrichment.py").read_text())

    def test_raw_schema_has_seven_fields(self):
        """Schema must match Phase 2 producer's contract exactly."""
        names = [f.name for f in enr.RAW_SCHEMA.fields]
        assert set(names) == {
            "hvfhs_license_num", "pickup_datetime", "dropoff_datetime",
            "PULocationID", "DOLocationID", "trip_miles", "trip_time",
        }, f"schema drift from producer contract: {names}"

    def test_topic_names(self):
        assert enr.SOURCE_TOPIC == "ride-events-raw"
        assert enr.SINK_TOPIC == "ride-events-enriched"


# ---------------------------------------------------------------------------
# Unit tests — small in-memory SparkSession, no Kafka, no Postgres
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """A minimal SparkSession for the unit tests. Module-scoped so the JVM
    cold-start cost is paid once for the whole class."""
    s = (
        SparkSession.builder
        .appName("test_enrichment")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/New_York")
        # Quiet down the JVM during tests
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def zones_df(spark: SparkSession):
    """A tiny static zones DataFrame — 3 zones, enough to test the join."""
    return spark.createDataFrame([
        (161, "Midtown Center",  "Manhattan", 40.762, -73.978),
        (162, "Midtown East",    "Manhattan", 40.755, -73.971),
        (236, "Upper East South", "Manhattan", 40.770, -73.962),
    ], schema="zone_id INT, zone_name STRING, borough STRING, "
              "centroid_lat DOUBLE, centroid_lon DOUBLE")


def _make_trip_df(spark: SparkSession, rows: list[tuple]):
    """Build a trips DF matching RAW_SCHEMA.

    `rows` is a list of (nyc_pickup_iso_string, PULocationID, DOLocationID).
    The ISO string is interpreted as NYC LOCAL time — we explicitly attach
    America/New_York tzinfo so Spark stores it as the correct UTC instant
    regardless of the test host's default timezone.
    """
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    NYC = ZoneInfo("America/New_York")

    expanded = []
    for nyc_iso, pu, do in rows:
        pickup = datetime.fromisoformat(nyc_iso).replace(tzinfo=NYC)
        dropoff = pickup + timedelta(minutes=10)
        expanded.append(Row(
            hvfhs_license_num="HV0003",
            pickup_datetime=pickup,
            dropoff_datetime=dropoff,
            PULocationID=pu,
            DOLocationID=do,
            trip_miles=2.5,
            trip_time=600.0,
        ))
    return spark.createDataFrame(expanded, schema=enr.RAW_SCHEMA)


class TestTemporalFeatures:
    """Verify temporal features are computed correctly in NYC time."""

    def test_hour_of_day_extracted_in_session_timezone(self, spark, zones_df):
        # 2024-07-15 14:30 NYC = a Monday afternoon
        trips = _make_trip_df(spark, [("2024-07-15T14:30:00", 161, 236)])
        result = enr.enrich(trips, zones_df).collect()[0]
        assert result.hour_of_day == 14

    def test_day_of_week_monday_is_2(self, spark, zones_df):
        """Spark's dayofweek: 1=Sun, 2=Mon, ..., 7=Sat."""
        trips = _make_trip_df(spark, [("2024-07-15T10:00:00", 161, 236)])  # Mon
        assert enr.enrich(trips, zones_df).collect()[0].day_of_week == 2

    def test_day_of_week_sunday_is_1(self, spark, zones_df):
        trips = _make_trip_df(spark, [("2024-07-14T10:00:00", 161, 236)])  # Sun
        assert enr.enrich(trips, zones_df).collect()[0].day_of_week == 1

    def test_is_weekend_saturday(self, spark, zones_df):
        trips = _make_trip_df(spark, [("2024-07-13T10:00:00", 161, 236)])  # Sat
        assert enr.enrich(trips, zones_df).collect()[0].is_weekend is True

    def test_is_weekend_friday(self, spark, zones_df):
        trips = _make_trip_df(spark, [("2024-07-12T10:00:00", 161, 236)])  # Fri
        assert enr.enrich(trips, zones_df).collect()[0].is_weekend is False


class TestRushHour:
    """The rush-hour boundary cases. Easy to get off-by-one."""

    def test_morning_rush_start(self, spark, zones_df):
        """7:00 AM Monday is rush hour."""
        trips = _make_trip_df(spark, [("2024-07-15T07:00:00", 161, 236)])
        assert enr.enrich(trips, zones_df).collect()[0].is_rush_hour is True

    def test_morning_rush_end_inclusive(self, spark, zones_df):
        """9:59 AM Monday is rush hour (between 7-9 inclusive on hour())."""
        trips = _make_trip_df(spark, [("2024-07-15T09:59:00", 161, 236)])
        assert enr.enrich(trips, zones_df).collect()[0].is_rush_hour is True

    def test_morning_rush_over(self, spark, zones_df):
        """10:00 AM Monday is NOT rush hour."""
        trips = _make_trip_df(spark, [("2024-07-15T10:00:00", 161, 236)])
        assert enr.enrich(trips, zones_df).collect()[0].is_rush_hour is False

    def test_evening_rush_start(self, spark, zones_df):
        """4:00 PM Monday is rush hour."""
        trips = _make_trip_df(spark, [("2024-07-15T16:00:00", 161, 236)])
        assert enr.enrich(trips, zones_df).collect()[0].is_rush_hour is True

    def test_evening_rush_end_inclusive(self, spark, zones_df):
        """7:59 PM Monday is rush hour (between 16-19 inclusive)."""
        trips = _make_trip_df(spark, [("2024-07-15T19:59:00", 161, 236)])
        assert enr.enrich(trips, zones_df).collect()[0].is_rush_hour is True

    def test_evening_rush_over(self, spark, zones_df):
        """8:00 PM Monday is NOT rush hour."""
        trips = _make_trip_df(spark, [("2024-07-15T20:00:00", 161, 236)])
        assert enr.enrich(trips, zones_df).collect()[0].is_rush_hour is False

    def test_weekend_rush_hour_excluded(self, spark, zones_df):
        """8 AM Saturday is NOT rush hour even though it's in the time window."""
        trips = _make_trip_df(spark, [("2024-07-13T08:00:00", 161, 236)])
        assert enr.enrich(trips, zones_df).collect()[0].is_rush_hour is False

    def test_midday_excluded(self, spark, zones_df):
        """12 noon Monday is not rush hour."""
        trips = _make_trip_df(spark, [("2024-07-15T12:00:00", 161, 236)])
        assert enr.enrich(trips, zones_df).collect()[0].is_rush_hour is False


class TestZoneJoin:
    """Verify zone metadata joins work correctly."""

    def test_pickup_zone_attached(self, spark, zones_df):
        trips = _make_trip_df(spark, [("2024-07-15T10:00:00", 161, 236)])
        result = enr.enrich(trips, zones_df).collect()[0]
        assert result.pu_zone_name == "Midtown Center"
        assert result.pu_borough == "Manhattan"
        assert result.pu_centroid_lat == 40.762

    def test_dropoff_zone_attached(self, spark, zones_df):
        trips = _make_trip_df(spark, [("2024-07-15T10:00:00", 161, 236)])
        result = enr.enrich(trips, zones_df).collect()[0]
        assert result.do_zone_name == "Upper East South"
        assert result.do_borough == "Manhattan"

    def test_unknown_pickup_zone_passes_through(self, spark, zones_df):
        """Trips with PULocationID not in zones table should NOT be dropped
        (LEFT join). Zone metadata is null."""
        trips = _make_trip_df(spark, [("2024-07-15T10:00:00", 999, 236)])
        results = enr.enrich(trips, zones_df).collect()
        assert len(results) == 1, "trip was dropped despite LEFT join"
        assert results[0].pu_zone_name is None
        assert results[0].PULocationID == 999

    def test_two_trips_two_pickup_zones(self, spark, zones_df):
        trips = _make_trip_df(spark, [
            ("2024-07-15T10:00:00", 161, 236),
            ("2024-07-15T11:00:00", 162, 161),
        ])
        results = sorted(
            enr.enrich(trips, zones_df).collect(),
            key=lambda r: r.PULocationID,
        )
        assert results[0].pu_zone_name == "Midtown Center"
        assert results[1].pu_zone_name == "Midtown East"


class TestKafkaPayload:
    """Verify the output is structured correctly for the Kafka sink."""

    def test_key_is_pulocationid(self, spark, zones_df):
        trips = _make_trip_df(spark, [("2024-07-15T10:00:00", 161, 236)])
        enriched = enr.enrich(trips, zones_df)
        out = enr.to_kafka_payload(enriched).collect()[0]
        assert out.key == "161"

    def test_value_is_valid_json(self, spark, zones_df):
        trips = _make_trip_df(spark, [("2024-07-15T10:00:00", 161, 236)])
        enriched = enr.enrich(trips, zones_df)
        out = enr.to_kafka_payload(enriched).collect()[0]
        parsed = json.loads(out.value)
        # Spot-check the shape
        assert parsed["PULocationID"] == 161
        assert parsed["pu_zone_name"] == "Midtown Center"
        assert parsed["hour_of_day"] == 10
        assert parsed["is_weekend"] is False
        assert parsed["is_rush_hour"] is False

    def test_value_contains_nyc_timestamp_string(self, spark, zones_df):
        trips = _make_trip_df(spark, [("2024-07-15T10:00:00", 161, 236)])
        enriched = enr.enrich(trips, zones_df)
        out = enr.to_kafka_payload(enriched).collect()[0]
        parsed = json.loads(out.value)
        # The NYC-local string should be present and human-readable
        assert "pickup_datetime_nyc" in parsed
        assert parsed["pickup_datetime_nyc"].startswith("2024-07-15")


# ---------------------------------------------------------------------------
# Slow integration test — runs real Spark + Kafka + Postgres
# ---------------------------------------------------------------------------

def _kafka_reachable() -> bool:
    try:
        from confluent_kafka.admin import AdminClient
        BROKERS = os.environ.get(
            "KAFKA_BROKERS",
            "localhost:9092,localhost:9093,localhost:9094",
        )
        AdminClient({"bootstrap.servers": BROKERS}).list_topics(timeout=5)
        return True
    except Exception:
        return False


def _postgres_reachable() -> bool:
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            dbname=os.environ.get("PG_DB", "rides"),
            user=os.environ.get("PG_USER", "rides"),
            password=os.environ.get("PG_PASSWORD", "rides"),
            connect_timeout=3,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM taxi_zones;")
            (n,) = cur.fetchone()
        conn.close()
        return n >= 260  # zones table populated
    except Exception:
        return False


def _has_real_parquet() -> bool:
    raw_dir = REPO_ROOT / "data" / "raw_trips"
    return raw_dir.is_dir() and any(raw_dir.glob("fhvhv_tripdata_*.parquet"))


@pytest.mark.slow
@pytest.mark.skipif(not _kafka_reachable(),
                    reason="Kafka cluster not reachable")
@pytest.mark.skipif(not _postgres_reachable(),
                    reason="Postgres not reachable or zones not loaded "
                           "(run scripts/load_zones.py)")
@pytest.mark.skipif(not _has_real_parquet(),
                    reason="no parquet in data/raw_trips/")
class TestEnrichmentIntegration:
    """End-to-end: launch enrichment, replay trips, verify enriched output."""

    def test_enriched_messages_appear(self, tmp_path):
        from confluent_kafka import Consumer

        BROKERS = os.environ.get(
            "KAFKA_BROKERS",
            "localhost:9092,localhost:9093,localhost:9094",
        )

        # --- Subscribe BEFORE launching enrichment so we don't miss messages ---
        group = f"enrichment-test-{uuid.uuid4().hex[:8]}"
        consumer = Consumer({
            "bootstrap.servers": BROKERS,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        consumer.subscribe(["ride-events-enriched"])
        deadline = time.time() + 10
        while not consumer.assignment() and time.time() < deadline:
            consumer.poll(0.5)
        assert consumer.assignment(), "consumer didn't get partition assignment"

        # --- Launch the enrichment job as a subprocess ---
        env = os.environ.copy()
        env["SPARK_MASTER"] = "local[2]"
        env["KAFKA_BROKERS"] = BROKERS
        env["CHECKPOINT_DIR"] = str(tmp_path / "checkpoints")
        env["PYSPARK_PYTHON"] = sys.executable

        spark_proc = subprocess.Popen(
            [sys.executable, str(SPARK_DIR / "enrichment.py")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        try:
            # --- Wait for "Streaming query 'enrichment' started" ---
            startup_deadline = time.time() + 120  # JVM + Maven + JDBC zones
            startup_ready = False
            log = []
            while time.time() < startup_deadline:
                line = spark_proc.stdout.readline()
                if not line:
                    if spark_proc.poll() is not None:
                        break
                    continue
                log.append(line)
                if "Streaming query" in line and "started" in line:
                    startup_ready = True
                    break
            assert startup_ready, (
                f"enrichment never started within 120s. Last 30 lines:\n"
                f"{''.join(log[-30:])}"
            )

            # --- Run replay producer ---
            parquet = next((REPO_ROOT / "data" / "raw_trips").glob("fhvhv_tripdata_*.parquet"))
            producer_result = subprocess.run(
                [sys.executable, str(REPO_ROOT / "scripts" / "replay_producer.py"),
                 "--file", str(parquet),
                 "--speed", "10000",
                 "--max-events", "100",
                 "--progress-every", "25"],
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert producer_result.returncode == 0, (
                f"producer failed: {producer_result.stderr}"
            )

            # --- Drain enriched topic for up to 30s ---
            enriched_messages = []
            deadline = time.time() + 30
            while time.time() < deadline and len(enriched_messages) < 5:
                msg = consumer.poll(timeout=1.0)
                if msg is None or msg.error():
                    continue
                enriched_messages.append(json.loads(msg.value()))

            assert len(enriched_messages) > 0, (
                "no enriched messages appeared on ride-events-enriched within 30s"
            )

            # --- Schema check on a sample message ---
            m = enriched_messages[0]
            for required in ("hvfhs_license_num", "pickup_datetime",
                             "PULocationID", "pu_zone_name", "pu_borough",
                             "hour_of_day", "is_rush_hour",
                             "pickup_datetime_nyc"):
                assert required in m, f"enriched msg missing field: {required}"
        finally:
            spark_proc.terminate()
            try:
                spark_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                spark_proc.kill()
            consumer.close()
