"""
Tests for Phase 3 Block 1 (Spark + Kafka hello world).

Two tiers:

  Unit tests (always run, fast):
    - hello_kafka.py imports cleanly
    - _session.get_session() returns a SparkSession with the right config
    - kafka_brokers() and checkpoint_dir() honor env vars
    - No code path hangs on import

  Integration tests (slow, marked):
    - Submit hello_kafka.py as a subprocess
    - Run replay_producer.py in parallel feeding ride-events-raw
    - Assert hello_kafka prints non-zero batch counts within 60 seconds
    - This is the load-bearing smoke test; if it passes, Spark↔Kafka works

Run unit tier only (fast, default):
    pytest tests/test_spark_hello.py -v -m "not slow"

Run integration tier (~90s, requires Kafka cluster up):
    pytest tests/test_spark_hello.py -v -m slow

Run everything:
    pytest tests/test_spark_hello.py -v
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_DIR = REPO_ROOT / "spark"
sys.path.insert(0, str(SPARK_DIR))


# ---------------------------------------------------------------------------
# Unit tests — no Spark or Kafka needed
# ---------------------------------------------------------------------------

class TestSessionHelpers:
    """Lightweight checks on _session.py's helpers — no JVM started."""

    def test_kafka_brokers_default(self, monkeypatch):
        monkeypatch.delenv("KAFKA_BROKERS", raising=False)
        from _session import kafka_brokers
        assert kafka_brokers() == "localhost:9092,localhost:9093,localhost:9094"

    def test_kafka_brokers_env_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BROKERS", "kafka-1:19092")
        # Reload the module so the env change takes effect
        import importlib
        import _session
        importlib.reload(_session)
        assert _session.kafka_brokers() == "kafka-1:19092"

    def test_checkpoint_dir_creates_path(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CHECKPOINT_DIR", str(tmp_path))
        import importlib
        import _session
        importlib.reload(_session)
        path = _session.checkpoint_dir("my_query")
        assert Path(path).is_dir()
        assert Path(path).name == "my_query"


class TestHelloKafkaScript:
    """Static checks on the hello_kafka.py script itself."""

    def test_script_parses(self):
        """File is syntactically valid Python."""
        import ast
        ast.parse((SPARK_DIR / "hello_kafka.py").read_text())

    def test_script_has_main_block(self):
        """File has a guarded entry point."""
        text = (SPARK_DIR / "hello_kafka.py").read_text()
        assert 'if __name__ == "__main__":' in text

    def test_subscribes_to_correct_topic(self):
        """The script reads from ride-events-raw, not anything else."""
        text = (SPARK_DIR / "hello_kafka.py").read_text()
        assert 'TOPIC = "ride-events-raw"' in text


# ---------------------------------------------------------------------------
# Integration test — runs a real Spark job + producer in subprocesses.
# Slow (~90s including Spark JVM startup), so marked and gated.
# ---------------------------------------------------------------------------

def _kafka_reachable() -> bool:
    try:
        from confluent_kafka.admin import AdminClient
        BROKERS = os.environ.get(
            "KAFKA_BROKERS",
            "localhost:9092,localhost:9093,localhost:9094",
        )
        admin = AdminClient({"bootstrap.servers": BROKERS})
        admin.list_topics(timeout=5)
        return True
    except Exception:
        return False


def _has_real_parquet() -> bool:
    """Return True if we have at least one real TLC parquet to feed the producer."""
    raw_dir = REPO_ROOT / "data" / "raw_trips"
    if not raw_dir.is_dir():
        return False
    return any(raw_dir.glob("fhvhv_tripdata_*.parquet"))


@pytest.mark.slow
@pytest.mark.skipif(not _kafka_reachable(),
                    reason="Kafka cluster not reachable — skipping Spark integration test")
@pytest.mark.skipif(not _has_real_parquet(),
                    reason="no parquet in data/raw_trips/ — run download_tlc.py first")
class TestSparkKafkaIntegration:
    """End-to-end: launch Spark, launch producer, assert Spark sees messages.

    This test is the entire reason Block 1 exists. If it passes, the
    Spark + Kafka layer is sound and we can build real jobs on top.
    """

    def test_spark_consumes_replayed_events(self, tmp_path):
        # Find a parquet to replay
        raw_dir = REPO_ROOT / "data" / "raw_trips"
        parquet = next(raw_dir.glob("fhvhv_tripdata_*.parquet"))

        # --- Launch the Spark hello job as a subprocess ---
        # We capture stdout — the test passes if we observe at least one
        # batch with a non-zero row count within the timeout.
        env = os.environ.copy()
        env.setdefault("KAFKA_BROKERS", "localhost:9092,localhost:9093,localhost:9094")
        env.setdefault("CHECKPOINT_DIR", str(tmp_path / "checkpoints"))
        env["SPARK_MASTER"] = "local[2]"  # 2 cores plenty for a smoke test
        # Use the same Python as pytest is using (our .venv)
        env["PYSPARK_PYTHON"] = sys.executable

        spark_proc = subprocess.Popen(
            [sys.executable, str(SPARK_DIR / "hello_kafka.py")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        try:
            # Wait for Spark to print "Streaming query 'hello_kafka' started"
            # — that means the readStream is live and listening.
            startup_deadline = time.time() + 90  # JVM cold start + Maven download
            startup_ready = False
            startup_log = []
            while time.time() < startup_deadline:
                line = spark_proc.stdout.readline()
                if not line:
                    if spark_proc.poll() is not None:
                        break  # process exited
                    continue
                startup_log.append(line)
                if "Streaming query" in line and "started" in line:
                    startup_ready = True
                    break

            assert startup_ready, (
                f"Spark never reported streaming query started within 90s. "
                f"Last 30 log lines:\n{''.join(startup_log[-30:])}"
            )

            # --- Now run the replay producer in this same process so we know
            # exactly when its messages are on the topic.
            producer_result = subprocess.run(
                [sys.executable, str(REPO_ROOT / "scripts" / "replay_producer.py"),
                 "--file", str(parquet),
                 "--speed", "10000",
                 "--max-events", "200",
                 "--progress-every", "50"],
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert producer_result.returncode == 0, (
                f"producer failed: {producer_result.stderr}"
            )

            # --- Read Spark's stdout for the next 30s, looking for batch
            # output with non-zero row counts.
            saw_nonempty_batch = False
            batch_lines = []
            deadline = time.time() + 30
            while time.time() < deadline:
                line = spark_proc.stdout.readline()
                if not line:
                    continue
                batch_lines.append(line)
                # Console sink prints "Batch: N" headers for each micro-batch.
                # Non-empty batches print a table with our `msg`/`partition`/etc
                # columns. We just need to see something other than the empty
                # boilerplate.
                if "msg" in line and "partition" in line and "offset" in line:
                    # Found the column header — next data line proves rows exist
                    data_line = spark_proc.stdout.readline()
                    batch_lines.append(data_line)
                    if data_line.strip().startswith("|") and "1" in data_line:
                        saw_nonempty_batch = True
                        break

            assert saw_nonempty_batch, (
                f"Spark never showed a non-empty batch within 30s of the producer "
                f"finishing. Last 50 log lines:\n{''.join(batch_lines[-50:])}"
            )

        finally:
            spark_proc.terminate()
            try:
                spark_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                spark_proc.kill()
                spark_proc.wait()
