#!/usr/bin/env python3
"""
Replay TLC parquet files into the ride-events-raw Kafka topic.

This is the entry point of Stage 2 — every downstream component (Spark
Enrichment, Aggregation, Forecasting, Network) consumes from ride-events-raw,
so the contract this script publishes is the contract the rest of the
pipeline depends on.

How replay works
----------------
TLC parquet files contain one row per real-world trip with a true
``pickup_datetime``. We read them in pickup-time order and publish each row
as a JSON message keyed by ``PULocationID`` (so every trip from the same
pickup zone lands on the same partition — important for stateful Spark
operations later).

The gap between consecutive ``pickup_datetime`` values is preserved (so
demand bursts in the data show up as bursts on the topic), but compressed
by ``--speed`` — at 100x, a real-world hour replays in 36 seconds.

Two flags control behavior at the boundaries:

  --loop          When the file ends, restart from the beginning instead
                  of exiting. Useful for thesis-day demos.

  --anchor now    Shift every published timestamp so the file's first row
                  appears to have happened "right now". Default; makes
                  Grafana's "last 1 hour" window work without any tweaks.

  --anchor original
                  Publish the file's true 2024 timestamps. Honest, but
                  every default Grafana time window will look empty
                  unless you remember to set it to "Jan 2024".

Examples
--------
Default — one file, 100x speed, anchored to "now", exits on EOF::

    python scripts/replay_producer.py --file data/raw_trips/fhvhv_tripdata_2024-01.parquet

Continuous demo — loop forever, 500x speed::

    python scripts/replay_producer.py --file data/raw_trips/fhvhv_tripdata_2024-01.parquet \\
        --speed 500 --loop

Only publish 1000 events then stop (smoke test / pipeline warm-up)::

    python scripts/replay_producer.py --file <...> --max-events 1000

Pipe stdout to a file to log message counts; cancel with Ctrl-C anytime
(the producer flushes pending messages before exiting).
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from confluent_kafka import KafkaError, Producer

# Local imports — match Phase 1 convention
from config import KAFKA_BROKERS, RAW_TRIPS_DIR
from logger import get_logger

log = get_logger("replay_producer")

TOPIC = "ride-events-raw"

# Columns we read out of the parquet. All other TLC columns (driver_pay,
# tolls, congestion_surcharge, etc.) are ignored by Stage 3 enrichment, so
# we drop them at source to keep messages small.
REQUIRED_COLUMNS = [
    "hvfhs_license_num",   # company code (HV0003=Uber, HV0005=Lyft)
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_time",
]


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

class _Shutdown:
    """Set by SIGINT/SIGTERM handlers; checked in the hot loop."""
    requested = False


def _install_signal_handlers() -> None:
    def handler(signum, frame):
        # Two ctrl-c presses = hard exit (in case flush hangs)
        if _Shutdown.requested:
            log.warning("second interrupt — exiting hard")
            sys.exit(130)
        log.info("shutdown requested — finishing current batch then flushing")
        _Shutdown.requested = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


# ---------------------------------------------------------------------------
# Producer setup
# ---------------------------------------------------------------------------

def make_producer(brokers: str) -> Producer:
    """
    Build a Kafka producer tuned for replay workloads.

    Tuning notes
    ------------
    * ``acks=all`` + ``enable.idempotence=true``: each message is written to
      every in-sync replica before being acked, and broker dedups retries.
      Slower than ``acks=1`` but means we won't lose or duplicate trips
      mid-replay.
    * ``linger.ms=20``: batch up to 20ms of messages before sending. At 100x
      replay this is ~30 messages per batch, way more efficient than
      one-message-per-RPC.
    * ``compression.type=zstd``: matches the broker-side topic config so
      messages stay compressed end-to-end.
    """
    return Producer({
        "bootstrap.servers": brokers,
        "client.id": "replay-producer",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "compression.type": "zstd",
        # Refuse to silently truncate messages — fail loudly if a row exceeds 1 MB
        "message.max.bytes": 1_048_576,
    })


def _delivery_callback(err: KafkaError | None, msg) -> None:
    """Logged on broker ack/nack. Called on the producer's poll thread."""
    if err is not None:
        log.error("delivery failed: topic=%s key=%s err=%s",
                  msg.topic(), msg.key(), err)


# ---------------------------------------------------------------------------
# Parquet reading
# ---------------------------------------------------------------------------

def load_trips(parquet_path: Path) -> pd.DataFrame:
    """
    Load a TLC parquet, keep only the fields we need, sort by pickup time.

    Sorting matters: replay logic assumes rows are in pickup-time order so
    the inter-arrival gaps it computes are non-negative. TLC files are
    almost always pre-sorted, but we don't rely on it.
    """
    log.info("reading %s", parquet_path)
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)

    # Read only the columns we publish — saves ~60% memory on FHVHV files.
    available = pq.ParquetFile(parquet_path).schema.names
    missing = [c for c in REQUIRED_COLUMNS if c not in available]
    if missing:
        raise ValueError(
            f"{parquet_path.name} is missing required columns: {missing}. "
            f"Available: {available}"
        )

    df = pd.read_parquet(parquet_path, columns=REQUIRED_COLUMNS)
    log.info("loaded %s rows", f"{len(df):,}")

    # Drop rows with null timestamps or zone IDs — they'd break enrichment
    # downstream anyway, easier to filter at the source.
    before = len(df)
    df = df.dropna(subset=["pickup_datetime", "PULocationID", "DOLocationID"])
    dropped = before - len(df)
    if dropped:
        log.warning("dropped %s rows with null pickup time or zone IDs", f"{dropped:,}")

    df = df.sort_values("pickup_datetime", kind="mergesort").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Time anchoring
# ---------------------------------------------------------------------------

def compute_anchor_offset(df: pd.DataFrame, anchor: str) -> timedelta:
    """
    Return the timedelta to ADD to every pickup_datetime/dropoff_datetime
    before publishing.

    anchor='original' -> 0 (publish as-is)
    anchor='now'      -> shift so the file's first pickup lands at "now"
    """
    if anchor == "original":
        return timedelta(0)
    if anchor == "now":
        first_pickup = pd.Timestamp(df["pickup_datetime"].iloc[0])
        # Strip tz if any so subtraction is unambiguous; we'll add UTC back
        # when serializing.
        if first_pickup.tzinfo is not None:
            first_pickup = first_pickup.tz_convert("UTC").tz_localize(None)
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        offset = now_utc - first_pickup.to_pydatetime()
        log.info("anchor=now: shifting timestamps by %s "
                 "(file's first pickup %s -> now %s)",
                 offset, first_pickup, now_utc)
        return offset
    raise ValueError(f"unknown anchor mode: {anchor!r}")


def shift_to_iso(ts, offset: timedelta) -> str:
    """Apply offset and return an ISO-8601 UTC string."""
    if pd.isna(ts):
        return None  # type: ignore[return-value]
    shifted = pd.Timestamp(ts) + offset
    if shifted.tzinfo is None:
        shifted = shifted.tz_localize("UTC")
    return shifted.isoformat()


# ---------------------------------------------------------------------------
# The replay loop itself
# ---------------------------------------------------------------------------

def build_message(row: pd.Series, offset: timedelta) -> tuple[bytes, bytes]:
    """Serialize one row into a (key, value) pair for Kafka."""
    payload = {
        "hvfhs_license_num": row["hvfhs_license_num"],
        "pickup_datetime":  shift_to_iso(row["pickup_datetime"],  offset),
        "dropoff_datetime": shift_to_iso(row["dropoff_datetime"], offset),
        "PULocationID": int(row["PULocationID"]),
        "DOLocationID": int(row["DOLocationID"]),
        "trip_miles": float(row["trip_miles"]) if pd.notna(row["trip_miles"]) else None,
        "trip_time":  float(row["trip_time"])  if pd.notna(row["trip_time"])  else None,
    }
    key = str(int(row["PULocationID"])).encode("utf-8")
    value = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return key, value


def replay_once(
    producer: Producer,
    df: pd.DataFrame,
    *,
    speed: float,
    anchor: str,
    max_events: int | None,
    progress_every: int,
) -> int:
    """
    Replay one full pass through ``df``. Returns the number of messages
    actually published in this pass.
    """
    offset = compute_anchor_offset(df, anchor)

    # Pre-compute inter-arrival gaps in seconds. We replay them divided by
    # speed; pre-computing avoids a Timestamp subtraction per row in the
    # hot loop.
    pickups = df["pickup_datetime"].astype("datetime64[ns]")
    gaps_seconds = (pickups.diff().dt.total_seconds().fillna(0.0) / speed).to_numpy()

    sent = 0
    start_wall = time.monotonic()

    for idx, row in enumerate(df.itertuples(index=False)):
        if _Shutdown.requested:
            break
        if max_events is not None and sent >= max_events:
            break

        # Sleep the (compressed) inter-arrival gap before sending this row.
        # gaps_seconds[0] == 0 by construction, so the first row is instant.
        gap = gaps_seconds[idx]
        if gap > 0:
            time.sleep(gap)

        # Build the message. row is a NamedTuple; convert via _asdict() to a
        # Series-like for the helper that already exists.
        # itertuples is faster than iterrows but loses Series semantics — we
        # inline the small bit we need.
        payload = {
            "hvfhs_license_num": row.hvfhs_license_num,
            "pickup_datetime":  shift_to_iso(row.pickup_datetime,  offset),
            "dropoff_datetime": shift_to_iso(row.dropoff_datetime, offset),
            "PULocationID": int(row.PULocationID),
            "DOLocationID": int(row.DOLocationID),
            "trip_miles": float(row.trip_miles) if pd.notna(row.trip_miles) else None,
            "trip_time":  float(row.trip_time)  if pd.notna(row.trip_time)  else None,
        }
        key = str(payload["PULocationID"]).encode("utf-8")
        value = json.dumps(payload, separators=(",", ":")).encode("utf-8")

        # produce() is async — librdkafka buffers internally and batches.
        # BufferError means the local queue is full; poll() to drain acks.
        while True:
            try:
                producer.produce(
                    topic=TOPIC,
                    key=key,
                    value=value,
                    on_delivery=_delivery_callback,
                )
                break
            except BufferError:
                producer.poll(0.1)

        # Drive delivery callbacks without blocking
        producer.poll(0)
        sent += 1

        if sent % progress_every == 0:
            elapsed = time.monotonic() - start_wall
            rate = sent / elapsed if elapsed > 0 else 0
            log.info("sent %s messages (%.0f msg/s wall time)", f"{sent:,}", rate)

    return sent


def run(args: argparse.Namespace) -> int:
    if not KAFKA_BROKERS:
        log.error("KAFKA_BROKERS is not set. Set it in .env or as an env var.")
        return 2

    parquet_path = Path(args.file)
    if not parquet_path.is_absolute():
        # Allow either a bare filename ("fhvhv_tripdata_2024-01.parquet") or
        # a relative path. Resolve against RAW_TRIPS_DIR for the bare case.
        if not parquet_path.exists() and (RAW_TRIPS_DIR / parquet_path).exists():
            parquet_path = RAW_TRIPS_DIR / parquet_path

    df = load_trips(parquet_path)
    if df.empty:
        log.error("no usable rows in %s", parquet_path)
        return 1

    producer = make_producer(KAFKA_BROKERS)
    _install_signal_handlers()

    log.info(
        "starting replay: topic=%s file=%s rows=%s speed=%sx anchor=%s loop=%s",
        TOPIC, parquet_path.name, f"{len(df):,}",
        args.speed, args.anchor, args.loop,
    )

    total_sent = 0
    pass_num = 0
    try:
        while True:
            pass_num += 1
            if args.loop:
                log.info("=== pass %d ===", pass_num)
            sent = replay_once(
                producer, df,
                speed=args.speed,
                anchor=args.anchor,
                max_events=args.max_events - total_sent if args.max_events else None,
                progress_every=args.progress_every,
            )
            total_sent += sent
            if _Shutdown.requested:
                break
            if args.max_events is not None and total_sent >= args.max_events:
                break
            if not args.loop:
                break
    finally:
        log.info("flushing %d pending messages...", len(producer))
        # Block until all in-flight messages have been delivered or timed out.
        unflushed = producer.flush(timeout=30)
        if unflushed:
            log.warning("%d messages still unflushed after 30s timeout", unflushed)
        log.info("done — total messages sent: %s", f"{total_sent:,}")

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--file", required=True,
        help="parquet file to replay. Bare filenames are resolved against "
             f"{RAW_TRIPS_DIR}",
    )
    p.add_argument(
        "--speed", type=float, default=100.0,
        help="replay speed multiplier — 1=real time, 100=fast demo, "
             "1000=as-fast-as-possible. Default 100.",
    )
    p.add_argument(
        "--anchor", choices=["now", "original"], default="now",
        help="'now' shifts timestamps so the file's first row lands at "
             "wall-clock now (Grafana-friendly). 'original' publishes the "
             "true source timestamps. Default 'now'.",
    )
    p.add_argument(
        "--loop", action="store_true",
        help="when the file ends, start over from the top. "
             "Default: exit cleanly.",
    )
    p.add_argument(
        "--max-events", type=int, default=None,
        help="stop after publishing this many messages total. Useful for "
             "smoke tests and pipeline warm-up.",
    )
    p.add_argument(
        "--progress-every", type=int, default=10_000,
        help="log a progress line every N messages. Default 10000.",
    )
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
