"""
Smoke test for the Phase 2 Kafka cluster.

Run after `docker compose up -d` and `python scripts/create_topics.py` to
verify that:

  1. All 3 brokers are discoverable.
  2. All 6 pipeline topics exist with the right partitions and replication.
  3. A round-trip produce+consume works end-to-end.

Run with:
    pytest tests/test_kafka_smoke.py -v
"""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

# Match the spec exactly — these are the source of truth for downstream code.
EXPECTED_TOPICS = {
    "ride-events-raw":      {"partitions": 12},
    "ride-events-enriched": {"partitions": 12},
    "demand-per-zone":      {"partitions": 6},
    "hotspot-alerts":       {"partitions": 3},
    "forecast-results":     {"partitions": 6},
    "weather-events":       {"partitions": 3},
}
EXPECTED_REPLICATION = 3
BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092,localhost:9093,localhost:9094")


@pytest.fixture(scope="module")
def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BROKERS, "client.id": "smoke-test"})


def test_cluster_has_three_brokers(admin: AdminClient) -> None:
    md = admin.list_topics(timeout=10)
    assert len(md.brokers) == 3, f"expected 3 brokers, found {len(md.brokers)}: {md.brokers}"


@pytest.mark.parametrize("name,spec", list(EXPECTED_TOPICS.items()))
def test_topic_exists_with_correct_config(admin: AdminClient, name: str, spec: dict) -> None:
    md = admin.list_topics(timeout=10)
    assert name in md.topics, f"topic {name!r} not found — did you run create_topics.py?"
    topic = md.topics[name]
    assert topic.error is None, f"topic {name} has error: {topic.error}"
    assert len(topic.partitions) == spec["partitions"], (
        f"{name}: expected {spec['partitions']} partitions, found {len(topic.partitions)}"
    )
    for p_id, p in topic.partitions.items():
        assert len(p.replicas) == EXPECTED_REPLICATION, (
            f"{name}/p{p_id}: expected RF={EXPECTED_REPLICATION}, found {len(p.replicas)}"
        )


def test_round_trip_produce_consume() -> None:
    """Publish a single message to ride-events-raw and read it back."""
    test_id = str(uuid.uuid4())
    topic = "ride-events-raw"
    group = f"smoke-{test_id[:8]}"

    # --- produce ---
    producer = Producer({"bootstrap.servers": BROKERS, "client.id": "smoke-producer"})
    payload = {"smoke_test_id": test_id, "ts": time.time()}
    producer.produce(topic, key="smoke", value=json.dumps(payload).encode())
    producer.flush(timeout=10)

    # --- consume ---
    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    found = False
    deadline = time.time() + 15
    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            data = json.loads(msg.value())
            if data.get("smoke_test_id") == test_id:
                found = True
                break
    finally:
        consumer.close()

    assert found, f"did not see test message with id {test_id} within 15s"
