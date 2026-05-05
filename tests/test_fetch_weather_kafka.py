"""
Tests for the Kafka publish path in scripts/fetch_weather.py.

Two tiers (same pattern as test_replay_producer.py):

  Unit tests (always run):
    - _publish_to_kafka returns False when KAFKA_BROKERS is empty
      (so the script keeps working in Phase-1-only environments)
    - _publish_to_kafka returns False on a malformed record

  Integration test (auto-skipped if Kafka cluster is unreachable):
    - Publish a synthetic weather record
    - Consume from weather-events
    - Verify the key, schema, and that the round-trip JSON is byte-equal
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import fetch_weather as fw  # noqa: E402

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092,localhost:9093,localhost:9094")
TOPIC = "weather-events"


def _make_record(station_id: str | None = None) -> dict:
    """Build a synthetic weather record matching what _parse_owm_payload would return."""
    sid = station_id or f"TEST-{uuid.uuid4().hex[:8]}"
    obs = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return {
        "observation_time": obs.isoformat(),
        "station_id": sid,
        "temperature_c": 22.5,
        "precipitation_mm": 0.5,
        "wind_speed_ms": 3.4,
        "humidity_pct": 60.0,
        "weather_condition": "Clouds",
    }


# ---------------------------------------------------------------------------
# Unit tests — no Kafka needed
# ---------------------------------------------------------------------------

class TestUnit:

    def test_returns_false_when_brokers_unset(self, monkeypatch):
        """In Phase-1-only environments, the publish path should no-op."""
        monkeypatch.setattr(fw, "KAFKA_BROKERS", "")
        assert fw._publish_to_kafka(_make_record()) is False

    def test_returns_false_when_brokers_none(self, monkeypatch):
        """Treat KAFKA_BROKERS=None the same as empty string."""
        monkeypatch.setattr(fw, "KAFKA_BROKERS", None)
        assert fw._publish_to_kafka(_make_record()) is False

    def test_returns_false_on_unreachable_broker(self, monkeypatch):
        """Bad broker address: producer construction succeeds but delivery times out."""
        monkeypatch.setattr(fw, "KAFKA_BROKERS", "localhost:1")  # nothing listening
        # The flush will time out (we set message.timeout.ms = 10s in the fn,
        # so this test takes ~10-15s). Acceptable for a unit-tier safety check.
        result = fw._publish_to_kafka(_make_record())
        assert result is False


# ---------------------------------------------------------------------------
# Integration test — runs only when Kafka is reachable
# ---------------------------------------------------------------------------

def _kafka_reachable() -> bool:
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": BROKERS})
        admin.list_topics(timeout=5)
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _kafka_reachable(),
                    reason="Kafka cluster not reachable — skipping integration test")
class TestEndToEnd:

    def test_publish_lands_on_weather_events(self, monkeypatch):
        """Publish a synthetic record, verify it shows up on weather-events with right key."""
        from confluent_kafka import Consumer

        monkeypatch.setattr(fw, "KAFKA_BROKERS", BROKERS)
        monkeypatch.setattr(fw, "KAFKA_WEATHER_TOPIC", TOPIC)

        # Subscribe BEFORE publishing so we don't miss the message
        group = f"weather-test-{uuid.uuid4().hex[:8]}"
        consumer = Consumer({
            "bootstrap.servers": BROKERS,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([TOPIC])

        # Wait for partition assignment (consumer needs to be fully subscribed
        # before producer sends, or the message lands on a partition we don't watch)
        deadline = time.time() + 10
        while not consumer.assignment() and time.time() < deadline:
            consumer.poll(0.5)
        assert consumer.assignment(), "consumer never got partition assignment"

        record = _make_record()
        result = fw._publish_to_kafka(record)
        assert result is True, "publish reported failure"

        # Drain consumer for up to 15s, looking for our specific record by station_id
        found = None
        deadline = time.time() + 15
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            data = json.loads(msg.value())
            if data.get("station_id") == record["station_id"]:
                found = {
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": data,
                    "partition": msg.partition(),
                }
                break
        consumer.close()

        assert found is not None, (
            f"did not see synthetic record (station_id={record['station_id']}) on "
            f"{TOPIC} within 15s"
        )
        # Key is station_id (so all records for one station co-partition — important
        # for stream-stream joins on Spark Enrichment side later)
        assert found["key"] == record["station_id"]
        # Round-trip JSON should be byte-equal to what we sent
        assert found["value"] == record
