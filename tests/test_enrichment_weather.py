"""
Tests for Phase 3 Block 3 (weather stream-stream join).

Three tiers:

  Static / unit (instant, no Spark, no Kafka):
    - generate_synthetic_weather produces the right schema
    - generate_synthetic_weather is reproducible with --seed
    - rain windows produce non-null precipitation_mm
    - schema constants in enrichment_weather match upstream contracts

  Unit with Spark (~30s, local SparkSession, no Kafka, no Postgres):
    - join_trips_with_weather attaches weather columns when in time window
    - emits NULL weather when no observation falls in the window
    - emits NULL when the only observation is OUTSIDE the time window
    - to_kafka_payload preserves all 24+6 fields and keys by PULocationID

  Slow integration (~3 min, requires full cluster):
    - Submit enrichment_weather.py as subprocess
    - Generate synthetic weather + run replay_producer in parallel
    - Verify ride-events-enriched-weather has trips with weather joined

Run unit:
    pytest tests/test_enrichment_weather.py -v -m "not slow"

Run slow integration:
    pytest tests/test_enrichment_weather.py -v -m slow
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_DIR = REPO_ROOT / "spark"
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SPARK_DIR))
sys.path.insert(0, str(SCRIPTS_DIR))

import enrichment_weather as ew  # noqa: E402
import generate_synthetic_weather as gen  # noqa: E402


# ---------------------------------------------------------------------------
# Tier 1: synthetic generator unit tests (no Spark needed)
# ---------------------------------------------------------------------------

class TestSyntheticGenerator:

    def test_produces_correct_count(self):
        start = datetime(2026, 5, 1, 0, tzinfo=timezone.utc)
        obs = gen.generate(start, hours=24, seed=42)
        assert len(obs) == 24

    def test_observations_are_hourly(self):
        start = datetime(2026, 5, 1, 0, tzinfo=timezone.utc)
        obs = gen.generate(start, hours=10, seed=42)
        times = [datetime.fromisoformat(o["observation_time"]) for o in obs]
        # Each successive observation should be exactly 1 hour later
        for i in range(1, len(times)):
            assert times[i] - times[i-1] == timedelta(hours=1)

    def test_schema_matches_fetch_weather(self):
        """Synthetic obs must have identical keys to fetch_weather.py's output."""
        obs = gen.generate(datetime(2026, 5, 1, tzinfo=timezone.utc),
                           hours=1, seed=42)[0]
        assert set(obs.keys()) == {
            "observation_time", "station_id", "temperature_c",
            "precipitation_mm", "wind_speed_ms", "humidity_pct",
            "weather_condition",
        }

    def test_seed_makes_output_reproducible(self):
        """Same seed = identical output. Important for test reliability."""
        start = datetime(2026, 5, 1, tzinfo=timezone.utc)
        a = gen.generate(start, hours=24, seed=12345)
        b = gen.generate(start, hours=24, seed=12345)
        assert a == b

    def test_different_seeds_diverge(self):
        start = datetime(2026, 5, 1, tzinfo=timezone.utc)
        a = gen.generate(start, hours=24, seed=1)
        b = gen.generate(start, hours=24, seed=2)
        # At least the temperatures should differ
        temps_a = [o["temperature_c"] for o in a]
        temps_b = [o["temperature_c"] for o in b]
        assert temps_a != temps_b

    def test_temperature_in_plausible_range(self):
        """Sanity: NYC weather isn't -40 or +50."""
        obs = gen.generate(datetime(2026, 5, 1, tzinfo=timezone.utc),
                           hours=48, seed=42)
        for o in obs:
            assert -10 <= o["temperature_c"] <= 40, \
                f"implausible temperature: {o['temperature_c']}"

    def test_humidity_in_valid_range(self):
        obs = gen.generate(datetime(2026, 5, 1, tzinfo=timezone.utc),
                           hours=48, seed=42)
        for o in obs:
            assert 0 <= o["humidity_pct"] <= 100

    def test_some_observations_have_rain(self):
        """With 48h, at least some hours should be rainy."""
        obs = gen.generate(datetime(2026, 5, 1, tzinfo=timezone.utc),
                           hours=48, seed=42)
        rainy = [o for o in obs if o["precipitation_mm"] is not None]
        assert len(rainy) > 0, "no rain at all in 48h is too dry to be realistic"

    def test_rain_observations_have_rain_condition(self):
        """When precipitation_mm is set, weather_condition should be Rain."""
        obs = gen.generate(datetime(2026, 5, 1, tzinfo=timezone.utc),
                           hours=48, seed=42)
        for o in obs:
            if o["precipitation_mm"] is not None:
                assert o["weather_condition"] == "Rain"

    def test_isoformat_timestamps(self):
        """Timestamps must round-trip through datetime.fromisoformat."""
        obs = gen.generate(datetime(2026, 5, 1, tzinfo=timezone.utc),
                           hours=5, seed=42)
        for o in obs:
            parsed = datetime.fromisoformat(o["observation_time"])
            assert parsed.tzinfo is not None  # must have timezone


# ---------------------------------------------------------------------------
# Tier 2: schema constants in enrichment_weather
# ---------------------------------------------------------------------------

class TestSchemaContracts:

    def test_weather_schema_matches_fetch_weather(self):
        """The streaming job's WEATHER_SCHEMA must match what synthetic
        and real producers emit."""
        producer_keys = {"observation_time", "station_id", "temperature_c",
                         "precipitation_mm", "wind_speed_ms", "humidity_pct",
                         "weather_condition"}
        schema_keys = {f.name for f in ew.WEATHER_SCHEMA.fields}
        assert schema_keys == producer_keys

    def test_enriched_trip_schema_includes_block2_fields(self):
        """Sanity: schema covers everything Block 2 emits."""
        names = {f.name for f in ew.ENRICHED_TRIP_SCHEMA.fields}
        for required in ("PULocationID", "pickup_datetime", "pu_zone_name",
                         "hour_of_day", "is_rush_hour", "pickup_datetime_nyc"):
            assert required in names, f"schema missing Block 2 field: {required}"

    def test_topic_names_match_pipeline_design(self):
        assert ew.TRIPS_TOPIC == "ride-events-enriched"
        assert ew.WEATHER_TOPIC == "weather-events"
        assert ew.SINK_TOPIC == "ride-events-enriched-weather"


# ---------------------------------------------------------------------------
# Tier 3: join logic on static DataFrames (Spark needed)
# ---------------------------------------------------------------------------
#
# We can't easily exercise watermark behavior in a static-DF unit test
# because watermarks are conceptually about late events arriving in
# subsequent micro-batches. But we CAN exercise the time-bound join
# condition itself, which is the higher-risk piece of logic.

@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder
        .appName("test_enrichment_weather")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _trip_row(pickup_iso_nyc: str, pu_id: int = 161, do_id: int = 236) -> dict:
    """One enriched trip row matching ENRICHED_TRIP_SCHEMA."""
    from zoneinfo import ZoneInfo
    NYC = ZoneInfo("America/New_York")
    pickup = datetime.fromisoformat(pickup_iso_nyc).replace(tzinfo=NYC)
    return {
        "hvfhs_license_num": "HV0003",
        "pickup_datetime": pickup,
        "dropoff_datetime": pickup + timedelta(minutes=10),
        "PULocationID": pu_id,
        "DOLocationID": do_id,
        "trip_miles": 2.5,
        "trip_time": 600.0,
        "pickup_datetime_nyc": pickup.strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_datetime_nyc": (pickup + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S"),
        "pu_zone_id": pu_id, "pu_zone_name": "Zone " + str(pu_id),
        "pu_borough": "Manhattan",
        "pu_centroid_lat": 40.762, "pu_centroid_lon": -73.978,
        "do_zone_id": do_id, "do_zone_name": "Zone " + str(do_id),
        "do_borough": "Manhattan",
        "do_centroid_lat": 40.770, "do_centroid_lon": -73.962,
        "hour_of_day": pickup.hour, "day_of_week": pickup.isoweekday() % 7 + 1,
        "is_weekend": pickup.weekday() >= 5,
        "is_rush_hour": (
            pickup.weekday() < 5 and (
                7 <= pickup.hour <= 9 or 16 <= pickup.hour <= 19
            )
        ),
    }


def _weather_row(obs_iso_utc: str, temp_c: float = 20.0,
                 precipitation_mm: float | None = None,
                 condition: str = "Clear") -> dict:
    """One weather row matching WEATHER_SCHEMA."""
    obs = datetime.fromisoformat(obs_iso_utc)
    if obs.tzinfo is None:
        obs = obs.replace(tzinfo=timezone.utc)
    return {
        "observation_time": obs,
        "station_id": "KNYC",
        "temperature_c": temp_c,
        "precipitation_mm": precipitation_mm,
        "wind_speed_ms": 5.0,
        "humidity_pct": 60.0,
        "weather_condition": condition,
    }


class TestJoinLogic:

    def test_weather_in_same_hour_attached(self, spark):
        """Weather observation in same hour bucket as pickup should match."""
        # Trip at 14:00 NYC = 18:00 UTC (during EDT)
        # Weather at 18:30 UTC = also bucket 18:00 UTC = matches
        trips = spark.createDataFrame([_trip_row("2026-07-15T14:00:00")],
                                      schema=ew.ENRICHED_TRIP_SCHEMA)
        weather = spark.createDataFrame([_weather_row("2026-07-15T18:30:00", 22.5)],
                                        schema=ew.WEATHER_SCHEMA)
        joined = ew.join_trips_with_weather(trips, weather)
        out = ew.to_kafka_payload(joined).collect()[0]
        payload = json.loads(out.value)
        assert payload["weather_temperature_c"] == 22.5

    def test_weather_in_different_hour_not_attached(self, spark):
        """Weather observation in a different hour bucket should NOT match."""
        trips = spark.createDataFrame([_trip_row("2026-07-15T14:00:00")],
                                      schema=ew.ENRICHED_TRIP_SCHEMA)
        # Trip bucket = 18:00 UTC. Weather at 15:00 UTC = bucket 15:00 UTC.
        weather = spark.createDataFrame([_weather_row("2026-07-15T15:00:00", 22.5)],
                                        schema=ew.WEATHER_SCHEMA)
        joined = ew.join_trips_with_weather(trips, weather)
        rows = ew.to_kafka_payload(joined).collect()
        # LEFT join — trip still present
        assert len(rows) == 1
        payload = json.loads(rows[0].value)
        assert payload["weather_temperature_c"] is None
        assert payload["PULocationID"] == 161  # trip data preserved

    def test_no_weather_at_all_emits_null_weather(self, spark):
        """Empty weather stream: trips pass through with null weather columns."""
        trips = spark.createDataFrame([_trip_row("2026-07-15T14:00:00")],
                                      schema=ew.ENRICHED_TRIP_SCHEMA)
        weather = spark.createDataFrame([], schema=ew.WEATHER_SCHEMA)
        joined = ew.join_trips_with_weather(trips, weather)
        rows = ew.to_kafka_payload(joined).collect()
        assert len(rows) == 1
        payload = json.loads(rows[0].value)
        assert payload["weather_temperature_c"] is None
        assert payload["weather_condition"] is None
        # But trip data is intact
        assert payload["pu_zone_name"] == "Zone 161"
        assert payload["hour_of_day"] == 14

    def test_weather_at_hour_boundary(self, spark):
        """Weather at the exact start of the hour bucket should match."""
        trips = spark.createDataFrame([_trip_row("2026-07-15T14:00:00")],
                                      schema=ew.ENRICHED_TRIP_SCHEMA)
        # Trip bucket = 18:00 UTC. Weather at exactly 18:00 UTC = matches.
        weather = spark.createDataFrame([_weather_row("2026-07-15T18:00:00", 25.0)],
                                        schema=ew.WEATHER_SCHEMA)
        joined = ew.join_trips_with_weather(trips, weather)
        payload = json.loads(ew.to_kafka_payload(joined).collect()[0].value)
        assert payload["weather_temperature_c"] == 25.0

    def test_rain_data_propagates(self, spark):
        """Rain weather in matching hour propagates to output."""
        trips = spark.createDataFrame([_trip_row("2026-07-15T14:00:00")],
                                      schema=ew.ENRICHED_TRIP_SCHEMA)
        # Trip bucket = 18:00 UTC. Weather at 18:45 UTC = same bucket.
        weather = spark.createDataFrame(
            [_weather_row("2026-07-15T18:45:00", temp_c=15.0,
                          precipitation_mm=2.5, condition="Rain")],
            schema=ew.WEATHER_SCHEMA,
        )
        payload = json.loads(
            ew.to_kafka_payload(ew.join_trips_with_weather(trips, weather))
            .collect()[0].value
        )
        assert payload["weather_precipitation_mm"] == 2.5
        assert payload["weather_condition"] == "Rain"


class TestKafkaPayloadShape:

    def test_key_is_pulocationid(self, spark):
        trips = spark.createDataFrame([_trip_row("2026-07-15T14:00:00", pu_id=42)],
                                      schema=ew.ENRICHED_TRIP_SCHEMA)
        weather = spark.createDataFrame([], schema=ew.WEATHER_SCHEMA)
        out = ew.to_kafka_payload(
            ew.join_trips_with_weather(trips, weather)
        ).collect()[0]
        assert out.key == "42"

    def test_value_is_valid_json_with_all_fields(self, spark):
        trips = spark.createDataFrame([_trip_row("2026-07-15T14:00:00")],
                                      schema=ew.ENRICHED_TRIP_SCHEMA)
        weather = spark.createDataFrame([_weather_row("2026-07-15T17:55:00")],
                                        schema=ew.WEATHER_SCHEMA)
        payload = json.loads(
            ew.to_kafka_payload(ew.join_trips_with_weather(trips, weather))
            .collect()[0].value
        )
        # All Block 2 fields preserved
        for k in ("hvfhs_license_num", "pickup_datetime", "PULocationID",
                  "pu_zone_name", "pu_borough", "hour_of_day",
                  "is_rush_hour", "pickup_datetime_nyc"):
            assert k in payload, f"missing block 2 field: {k}"
        # All Block 3 weather fields added
        for k in ("weather_temperature_c", "weather_precipitation_mm",
                  "weather_wind_speed_ms", "weather_humidity_pct",
                  "weather_condition", "weather_observation_time"):
            assert k in payload, f"missing block 3 weather field: {k}"


# ---------------------------------------------------------------------------
# Tier 4: slow integration test
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


def _has_real_parquet() -> bool:
    raw_dir = REPO_ROOT / "data" / "raw_trips"
    return raw_dir.is_dir() and any(raw_dir.glob("fhvhv_tripdata_*.parquet"))


@pytest.mark.slow
@pytest.mark.skipif(not _kafka_reachable(), reason="Kafka not reachable")
@pytest.mark.skipif(not _has_real_parquet(), reason="no parquet in data/raw_trips/")
class TestEnrichmentWeatherIntegration:

    def test_weather_enriched_messages_appear(self, tmp_path):
        from confluent_kafka import Consumer

        BROKERS = os.environ.get(
            "KAFKA_BROKERS",
            "localhost:9092,localhost:9093,localhost:9094",
        )

        # Subscribe to the OUTPUT topic before launching anything
        group = f"weather-enrich-test-{uuid.uuid4().hex[:8]}"
        consumer = Consumer({
            "bootstrap.servers": BROKERS,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        consumer.subscribe(["ride-events-enriched-weather"])
        deadline = time.time() + 10
        while not consumer.assignment() and time.time() < deadline:
            consumer.poll(0.5)
        assert consumer.assignment(), "consumer didn't get partition assignment"

        env = os.environ.copy()
        env["SPARK_MASTER"] = "local[2]"
        env["KAFKA_BROKERS"] = BROKERS
        env["CHECKPOINT_DIR"] = str(tmp_path / "checkpoints")
        env["PYSPARK_PYTHON"] = sys.executable

        # Step 1: publish synthetic weather covering "now" ± 24h
        synth_result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "generate_synthetic_weather.py"),
             "--hours", "48", "--seed", "42"],
            env=env, capture_output=True, text=True, timeout=60,
        )
        assert synth_result.returncode == 0, (
            f"synthetic weather failed: {synth_result.stderr}"
        )

        # Step 2: launch the weather enrichment job
        spark_proc = subprocess.Popen(
            [sys.executable, str(SPARK_DIR / "enrichment_weather.py")],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )

        try:
            # Wait for "Streaming query 'enrichment_weather' started"
            startup_deadline = time.time() + 90
            startup_log = []
            ready = False
            while time.time() < startup_deadline:
                line = spark_proc.stdout.readline()
                if not line:
                    if spark_proc.poll() is not None:
                        break
                    continue
                startup_log.append(line)
                if "Streaming query" in line and "started" in line:
                    ready = True
                    break
            assert ready, (
                f"enrichment_weather never started. Last 30 log lines:\n"
                f"{''.join(startup_log[-30:])}"
            )

            # Step 3: replay trips so the join has both sides
            parquet = next((REPO_ROOT / "data" / "raw_trips").glob(
                "fhvhv_tripdata_*.parquet"))
            trip_proc = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "replay_producer.py"),
                 "--file", str(parquet),
                 "--speed", "10000",
                 "--max-events", "200"],
                env=env, capture_output=True, text=True, timeout=60,
            )
            assert trip_proc.returncode == 0, f"trip replay failed: {trip_proc.stderr}"

            # Step 4: drain enriched-weather topic
            # Note: stream-stream join with watermarks delays output, so we
            # need a longer drain than the trip-only test. ~watermark + a
            # batch is the realistic minimum.
            messages = []
            deadline = time.time() + 90  # generous — watermark is 30 min
                                          # but Spark also flushes on micro-batch boundaries
            while time.time() < deadline and len(messages) < 1:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                messages.append(json.loads(msg.value()))

            assert len(messages) > 0, (
                "no weather-enriched messages within 90s. "
                "Watermark delays can cause this; check spark stdout for batch progress."
            )

            # Schema check on a sample
            m = messages[0]
            for required in ("PULocationID", "pu_zone_name",
                             "weather_temperature_c", "weather_condition",
                             "weather_observation_time"):
                assert required in m, f"missing field: {required}"
        finally:
            spark_proc.terminate()
            try:
                spark_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                spark_proc.kill()
            consumer.close()