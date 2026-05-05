"""
Tests for scripts/replay_producer.py.

Two tiers:

  Unit tests (always run):
    - CLI parsing
    - load_trips() drops nulls, sorts by pickup time, validates columns
    - compute_anchor_offset() math is correct for both modes

  Integration tests (require live Kafka — auto-skip if cluster down):
    - Generate a tiny synthetic FHVHV parquet
    - Run the replay producer against it (--max-events to bound runtime)
    - Consume from ride-events-raw, verify message count, keys, and that
      the JSON payload matches what we put in
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

# Path setup so we can import scripts/replay_producer
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import replay_producer as rp  # noqa: E402

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092,localhost:9093,localhost:9094")
TOPIC = "ride-events-raw"


# ---------------------------------------------------------------------------
# Fixture: a tiny synthetic parquet matching the FHVHV schema we care about
# ---------------------------------------------------------------------------

@pytest.fixture
def tiny_parquet(tmp_path: Path) -> Path:
    """5 rows, 1-minute pickup gaps, 2 zones — enough to test all logic."""
    df = pd.DataFrame({
        "hvfhs_license_num": ["HV0003"] * 5,
        "pickup_datetime":  pd.to_datetime([
            "2024-01-15 08:00:00",
            "2024-01-15 08:01:00",
            "2024-01-15 08:02:00",
            "2024-01-15 08:03:00",
            "2024-01-15 08:04:00",
        ]),
        "dropoff_datetime": pd.to_datetime([
            "2024-01-15 08:10:00",
            "2024-01-15 08:11:00",
            "2024-01-15 08:12:00",
            "2024-01-15 08:13:00",
            "2024-01-15 08:14:00",
        ]),
        "PULocationID": [161, 162, 161, 162, 161],
        "DOLocationID": [100, 101, 102, 103, 104],
        "trip_miles":   [2.5, 3.0, 1.8, 4.2, 2.1],
        "trip_time":    [600, 720, 540, 900, 660],
    })
    out = tmp_path / "fhvhv_tripdata_2024-01.parquet"
    df.to_parquet(out, index=False)
    return out


# ---------------------------------------------------------------------------
# Unit tests — no Kafka needed
# ---------------------------------------------------------------------------

class TestCLI:

    def test_required_file_arg(self):
        with pytest.raises(SystemExit):
            rp.parse_args([])  # missing --file

    def test_defaults(self):
        args = rp.parse_args(["--file", "x.parquet"])
        assert args.speed == 100.0
        assert args.anchor == "now"
        assert args.loop is False
        assert args.max_events is None

    def test_loop_and_anchor_original(self):
        args = rp.parse_args(["--file", "x.parquet", "--loop", "--anchor", "original"])
        assert args.loop is True
        assert args.anchor == "original"

    def test_invalid_anchor_rejected(self):
        with pytest.raises(SystemExit):
            rp.parse_args(["--file", "x.parquet", "--anchor", "yesterday"])


class TestLoadTrips:

    def test_loads_and_sorts(self, tiny_parquet: Path):
        df = rp.load_trips(tiny_parquet)
        assert len(df) == 5
        # Already sorted in fixture, but verify load_trips guarantees it
        assert df["pickup_datetime"].is_monotonic_increasing

    def test_drops_null_pickup(self, tmp_path: Path):
        df = pd.DataFrame({
            "hvfhs_license_num": ["HV0003"] * 3,
            "pickup_datetime":   [pd.Timestamp("2024-01-15 08:00"), pd.NaT, pd.Timestamp("2024-01-15 08:02")],
            "dropoff_datetime":  pd.to_datetime(["2024-01-15 08:10", "2024-01-15 08:11", "2024-01-15 08:12"]),
            "PULocationID": [161, 162, 161],
            "DOLocationID": [100, 101, 102],
            "trip_miles":   [2.5, 3.0, 1.8],
            "trip_time":    [600, 720, 540],
        })
        path = tmp_path / "with_null.parquet"
        df.to_parquet(path, index=False)
        result = rp.load_trips(path)
        assert len(result) == 2

    def test_missing_column_raises(self, tmp_path: Path):
        df = pd.DataFrame({"only_one_column": [1, 2, 3]})
        path = tmp_path / "bad_schema.parquet"
        df.to_parquet(path, index=False)
        with pytest.raises(ValueError, match="missing required columns"):
            rp.load_trips(path)


class TestAnchorOffset:

    def test_original_returns_zero(self, tiny_parquet: Path):
        df = rp.load_trips(tiny_parquet)
        offset = rp.compute_anchor_offset(df, "original")
        assert offset.total_seconds() == 0

    def test_now_shifts_first_row_to_now(self, tiny_parquet: Path):
        df = rp.load_trips(tiny_parquet)
        before = datetime.now(timezone.utc).replace(tzinfo=None)
        offset = rp.compute_anchor_offset(df, "now")
        after = datetime.now(timezone.utc).replace(tzinfo=None)

        first_pickup = pd.Timestamp(df["pickup_datetime"].iloc[0]).to_pydatetime()
        shifted = first_pickup + offset

        # Shifted first pickup should be between when we entered and when we left
        # the function, give or take a tolerance for the function's own runtime.
        assert before <= shifted <= after + (after - before)

    def test_unknown_anchor_raises(self, tiny_parquet: Path):
        df = rp.load_trips(tiny_parquet)
        with pytest.raises(ValueError, match="unknown anchor"):
            rp.compute_anchor_offset(df, "tomorrow")


# ---------------------------------------------------------------------------
# Integration test — needs a running Kafka cluster
# ---------------------------------------------------------------------------

def _kafka_reachable() -> bool:
    """True if we can list topics from the cluster within 5s."""
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

    def test_replay_publishes_to_kafka(self, tiny_parquet: Path):
        from confluent_kafka import Consumer

        # --- Set up a consumer subscribed to ride-events-raw BEFORE the
        # producer runs, so we don't miss messages.
        group = f"replay-test-{uuid.uuid4().hex[:8]}"
        consumer = Consumer({
            "bootstrap.servers": BROKERS,
            "group.id": group,
            "auto.offset.reset": "latest",  # only see messages produced AFTER we subscribe
            "enable.auto.commit": False,
        })
        consumer.subscribe([TOPIC])
        # Trigger partition assignment (necessary before any produce happens)
        deadline = time.time() + 10
        while not consumer.assignment() and time.time() < deadline:
            consumer.poll(0.5)
        assert consumer.assignment(), "consumer never got partition assignment"

        # --- Run the replay producer as a subprocess. Speed=10000 means the
        # 4 minutes of synthetic gaps replay in well under a second.
        env = os.environ.copy()
        env["KAFKA_BROKERS"] = BROKERS
        env["PYTHONPATH"] = str(REPO_ROOT / "scripts")
        result = subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts" / "replay_producer.py"),
             "--file", str(tiny_parquet),
             "--speed", "10000",
             "--anchor", "original",
             "--max-events", "5",
             "--progress-every", "1"],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, (
            f"producer failed: stdout={result.stdout!r} stderr={result.stderr!r}"
        )

        # --- Drain the consumer for up to 15 seconds, collecting messages.
        received = []
        deadline = time.time() + 15
        while time.time() < deadline and len(received) < 5:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            received.append({
                "key": msg.key().decode() if msg.key() else None,
                "value": json.loads(msg.value()),
                "partition": msg.partition(),
            })
        consumer.close()

        # --- Assertions
        assert len(received) == 5, f"expected 5 messages, got {len(received)}"

        # Keys should be the PULocationIDs we put in (as strings)
        expected_keys = ["161", "162", "161", "162", "161"]
        # Order isn't guaranteed across partitions, but all keys-with-the-same-value
        # go to the same partition, so key 161 messages stay in order, key 162 same.
        assert sorted(m["key"] for m in received) == sorted(expected_keys)

        # Schema check on the JSON payload
        for m in received:
            v = m["value"]
            assert set(v.keys()) == {
                "hvfhs_license_num", "pickup_datetime", "dropoff_datetime",
                "PULocationID", "DOLocationID", "trip_miles", "trip_time",
            }
            assert v["hvfhs_license_num"] == "HV0003"
            assert isinstance(v["PULocationID"], int)
            assert isinstance(v["DOLocationID"], int)
            # anchor=original means we should see 2024 timestamps as-is
            assert v["pickup_datetime"].startswith("2024-01-15T08:")
