#!/usr/bin/env python3
"""
Create all Kafka topics required by the ride-hailing pipeline.

Auto-create is disabled at the broker level (see docker-compose.yml), so every
topic must be created explicitly with the right partitions, replication, and
retention. This script is the single source of truth for that configuration.

It is idempotent — running it twice does nothing the second time.

Usage:
    python scripts/create_topics.py              # create any missing topics
    python scripts/create_topics.py --describe   # print current state, no changes
    python scripts/create_topics.py --reset      # DELETE all topics then recreate
                                                 #   (asks for confirmation)

Spec reference: Project_Preview_Pipeline.pdf, Stage 2 (page 7).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource

# Bootstrap servers default to the host-side EXTERNAL listener so this script
# works whether it's run on the host (during dev) or from inside a container
# (override with KAFKA_BROKERS env var to use INTERNAL listener).
DEFAULT_BROKERS = "localhost:9092,localhost:9093,localhost:9094"


# -----------------------------------------------------------------------------
# Topic specs — keep in sync with Project_Preview_Pipeline.pdf page 7
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    key_field: str               # documentation only; producers set the key
    retention_ms: int            # how long to keep messages
    purpose: str

    @property
    def retention_human(self) -> str:
        h = self.retention_ms // 3_600_000
        return f"{h // 24}d" if h >= 24 else f"{h}h"


TOPICS: list[TopicSpec] = [
    TopicSpec("ride-events-raw",      12, "PULocationID",   24 * 3_600_000,
              "Raw trip events from replay_producer.py"),
    TopicSpec("ride-events-enriched", 12, "PULocationID",   48 * 3_600_000,
              "Trip events after Spark enrichment (zone names, weather, features)"),
    TopicSpec("demand-per-zone",       6, "zone_id",      7 * 24 * 3_600_000,
              "15-min windowed pickup/dropoff counts per zone"),
    TopicSpec("hotspot-alerts",        3, "borough",        24 * 3_600_000,
              "Alerts when zone demand exceeds 2x the 7-day baseline"),
    TopicSpec("forecast-results",      6, "zone_id",      7 * 24 * 3_600_000,
              "ARIMA/ETS predictions with confidence intervals"),
    TopicSpec("weather-events",        3, "station_id",     48 * 3_600_000,
              "Hourly weather observations from fetch_weather.py"),
    TopicSpec("ride-events-enriched-weather", 12, "PULocationID", 48 * 3_600_000,
          "Trip events fully enriched with zone metadata + weather"),
    TopicSpec("network-flow-updates",  6, "origin_zone_id", 7 * 24 * 3_600_000,
              "Hourly OD pair flow counts from Spark Network job"),
]

# Replication factor 3 matches our 3-broker cluster. min.insync.replicas=2
# means at least 2 brokers must ack writes — survives one broker failing.
REPLICATION_FACTOR = 3
MIN_INSYNC_REPLICAS = "2"


# -----------------------------------------------------------------------------
# AdminClient helpers
# -----------------------------------------------------------------------------

def get_admin(brokers: str) -> AdminClient:
    return AdminClient({
        "bootstrap.servers": brokers,
        "client.id": "topic-init",
    })


def list_existing(admin: AdminClient) -> dict[str, dict]:
    """Return {topic_name: {partitions: int, replication_factor: int}}."""
    md = admin.list_topics(timeout=10)
    return {
        name: {
            "partitions": len(t.partitions),
            "replication_factor": len(next(iter(t.partitions.values())).replicas)
                if t.partitions else 0,
        }
        for name, t in md.topics.items()
        if not name.startswith("_")  # hide internal topics like __consumer_offsets
    }


def topic_config(spec: TopicSpec) -> dict[str, str]:
    return {
        "retention.ms": str(spec.retention_ms),
        "min.insync.replicas": MIN_INSYNC_REPLICAS,
        # Compression saves ~70% on disk for the JSON payloads we're sending.
        # zstd is faster than gzip and compresses better than snappy.
        "compression.type": "zstd",
    }


def create_missing(admin: AdminClient, missing: list[TopicSpec]) -> int:
    if not missing:
        return 0
    new_topics = [
        NewTopic(
            topic=s.name,
            num_partitions=s.partitions,
            replication_factor=REPLICATION_FACTOR,
            config=topic_config(s),
        )
        for s in missing
    ]
    futures = admin.create_topics(new_topics, request_timeout=15)
    failures = 0
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"  ✓ created  {name}")
        except Exception as e:
            print(f"  ✗ FAILED   {name}: {e}", file=sys.stderr)
            failures += 1
    return failures


def delete_topics(admin: AdminClient, names: list[str]) -> int:
    if not names:
        return 0
    futures = admin.delete_topics(names, operation_timeout=15)
    failures = 0
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"  ✓ deleted  {name}")
        except Exception as e:
            print(f"  ✗ FAILED   {name}: {e}", file=sys.stderr)
            failures += 1
    # Deletion is async on the broker side — wait briefly for it to settle.
    time.sleep(2)
    return failures


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def cmd_describe(admin: AdminClient) -> int:
    existing = list_existing(admin)
    print(f"\n{'topic':<24} {'parts':>6} {'rf':>4} {'retention':>11}  status")
    print("-" * 70)
    for s in TOPICS:
        if s.name in existing:
            e = existing[s.name]
            ok = e["partitions"] == s.partitions and e["replication_factor"] == REPLICATION_FACTOR
            mark = "OK" if ok else "MISMATCH"
            print(f"{s.name:<24} {e['partitions']:>6} {e['replication_factor']:>4} "
                  f"{s.retention_human:>11}  {mark}")
        else:
            print(f"{s.name:<24} {'-':>6} {'-':>4} {s.retention_human:>11}  MISSING")
    print()
    return 0


def cmd_create(admin: AdminClient) -> int:
    existing = set(list_existing(admin).keys())
    missing = [s for s in TOPICS if s.name not in existing]
    already = [s.name for s in TOPICS if s.name in existing]
    if already:
        print(f"already present: {', '.join(already)}")
    if not missing:
        print("nothing to do — all 6 topics already exist.")
        return 0
    print(f"creating {len(missing)} topic(s):")
    fails = create_missing(admin, missing)
    print()
    return cmd_describe(admin) or (1 if fails else 0)


def cmd_reset(admin: AdminClient, yes: bool) -> int:
    existing = [s.name for s in TOPICS if s.name in list_existing(admin)]
    if not existing:
        print("no topics to delete; running create instead.")
        return cmd_create(admin)
    print("This will DELETE the following topics and all their data:")
    for name in existing:
        print(f"  - {name}")
    if not yes:
        resp = input("\nType 'yes' to continue: ").strip().lower()
        if resp != "yes":
            print("aborted.")
            return 1
    print("\ndeleting:")
    delete_topics(admin, existing)
    print("\nrecreating:")
    return cmd_create(admin)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--describe", action="store_true",
                   help="print current topic state without changes")
    p.add_argument("--reset", action="store_true",
                   help="DELETE all pipeline topics, then recreate them")
    p.add_argument("--yes", action="store_true",
                   help="skip the --reset confirmation prompt")
    p.add_argument("--brokers", default=os.environ.get("KAFKA_BROKERS", DEFAULT_BROKERS),
                   help="comma-separated bootstrap servers "
                        f"(default: $KAFKA_BROKERS or {DEFAULT_BROKERS})")
    args = p.parse_args()

    print(f"connecting to Kafka at {args.brokers}")
    admin = get_admin(args.brokers)

    # Verify the cluster is actually reachable before doing anything.
    try:
        md = admin.list_topics(timeout=5)
        n_brokers = len(md.brokers)
        print(f"cluster is up — {n_brokers} broker(s) discovered\n")
    except Exception as e:
        print(f"cannot reach Kafka: {e}", file=sys.stderr)
        print("is the cluster running? try: docker compose ps", file=sys.stderr)
        return 2

    if args.describe:
        return cmd_describe(admin)
    if args.reset:
        return cmd_reset(admin, yes=args.yes)
    return cmd_create(admin)


if __name__ == "__main__":
    sys.exit(main())
