#!/usr/bin/env python3
"""
scripts/weather_replay_producer.py

Replay observations from data/weather/hourly_weather.csv into the
weather-events Kafka topic. Mirrors scripts/replay_producer.py for trips,
but for weather data and adapted to the different volume profile (~1
observation per hour vs millions of trips).

Why this exists
---------------
Phase 1's fetch_weather.py runs hourly via cron and writes to a CSV. After
weeks of operation you have a real CSV with real observations. This script
plays them back to Kafka so:

  - Block 3's enrichment_weather job can join trips with the actual weather
    that occurred when those trips happened (--anchor original)
  - Or, demo mode: shift timestamps so the historical observations appear
    to be "right now" (--anchor now), pairing with replay_producer.py's
    --anchor now for trips

Use the synthetic generator (generate_synthetic_weather.py) instead when:
  - Your CSV is empty or sparse
  - You want reproducible test data
  - You want to model specific weather scenarios

Examples
--------
Replay all weather, shifting the first observation to 'now' (matches
replay_producer.py --anchor now defaults)::

    python scripts/weather_replay_producer.py

Replay with original 2024 timestamps (use with --anchor original on trips
producer to keep them aligned)::

    python scripts/weather_replay_producer.py --anchor original

As fast as possible (default 1.0 = real-time hourly cadence; for hourly
data you almost always want --speed > 1)::

    python scripts/weather_replay_producer.py --speed 100
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import KAFKA_BROKERS, WEATHER_CSV_PATH  # noqa: E402
from logger import get_logger                       # noqa: E402

log = get_logger("weather_replay_producer")

WEATHER_TOPIC = "weather-events"


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

def load_weather(csv_path: Path) -> pd.DataFrame:
    log.info("reading %s", csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    df = pd.read_csv(csv_path)
    log.info("loaded %d rows", len(df))

    required = {"observation_time", "station_id", "temperature_c",
                "precipitation_mm", "wind_speed_ms", "humidity_pct",
                "weather_condition"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")

    df["observation_time"] = pd.to_datetime(df["observation_time"], utc=True)
    df = df.dropna(subset=["observation_time"])
    df = df.sort_values("observation_time", kind="mergesort").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Anchoring (mirrors replay_producer.py)
# ---------------------------------------------------------------------------

def compute_anchor_offset(df: pd.DataFrame, anchor: str) -> timedelta:
    if anchor == "original":
        return timedelta(0)
    if anchor == "now":
        first = pd.Timestamp(df["observation_time"].iloc[0])
        if first.tzinfo is not None:
            first = first.tz_convert("UTC").tz_localize(None)
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        offset = now_utc - first.to_pydatetime()
        log.info("anchor=now: shifting timestamps by %s", offset)
        return offset
    raise ValueError(f"unknown anchor mode: {anchor!r}")


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

def make_producer(brokers: str):
    from confluent_kafka import Producer
    return Producer({
        "bootstrap.servers": brokers,
        "client.id": "weather-replay-producer",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
    })


def _row_to_payload(row: pd.Series, offset: timedelta) -> dict:
    """One CSV row -> one Kafka message payload, schema matches fetch_weather.py."""
    obs_time = pd.Timestamp(row["observation_time"]) + offset
    if obs_time.tzinfo is None:
        obs_time = obs_time.tz_localize("UTC")

    def _none_if_nan(v):
        if pd.isna(v):
            return None
        try:
            if isinstance(v, float) and math.isnan(v):
                return None
        except TypeError:
            pass
        return v

    return {
        "observation_time": obs_time.isoformat(),
        "station_id": str(row["station_id"]),
        "temperature_c": _none_if_nan(row["temperature_c"]),
        "precipitation_mm": _none_if_nan(row["precipitation_mm"]),
        "wind_speed_ms": _none_if_nan(row["wind_speed_ms"]),
        "humidity_pct": _none_if_nan(row["humidity_pct"]),
        "weather_condition": _none_if_nan(row["weather_condition"]),
    }


def replay(producer, df: pd.DataFrame, *, speed: float, anchor: str) -> int:
    offset = compute_anchor_offset(df, anchor)

    # Inter-arrival gaps in seconds, divided by speed.
    gaps = (df["observation_time"].diff().dt.total_seconds().fillna(0.0) / speed).to_numpy()


    sent = 0
    for idx, row in enumerate(df.itertuples(index=False)):
        gap = gaps[idx]
        if gap > 0:
            time.sleep(gap)
        payload = _row_to_payload(pd.Series(row._asdict()), offset)
        producer.produce(
            topic=WEATHER_TOPIC,
            key=payload["station_id"].encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
        )
        producer.poll(0)
        sent += 1
    return sent


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--csv", type=Path, default=Path(WEATHER_CSV_PATH),
                   help=f"weather CSV to replay. Default {WEATHER_CSV_PATH}")
    p.add_argument("--speed", type=float, default=100000.0,
                   help="replay speed multiplier. Weather is hourly, so even "
                        "speed=1 takes hours between messages. Default 1000.")
    p.add_argument("--anchor", choices=["now", "original"], default="now",
                   help="'now' shifts timestamps so the first row lands at "
                        "wall-clock now. 'original' keeps source timestamps. "
                        "Default 'now' — match replay_producer.py default.")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    if not KAFKA_BROKERS:
        log.error("KAFKA_BROKERS not set.")
        return 2

    df = load_weather(args.csv)
    if df.empty:
        log.error("no rows in %s", args.csv)
        return 1

    producer = make_producer(KAFKA_BROKERS)
    log.info("replaying weather: rows=%d speed=%sx anchor=%s",
             len(df), args.speed, args.anchor)

    sent = replay(producer, df, speed=args.speed, anchor=args.anchor)
    log.info("flushing pending messages...")
    producer.flush(timeout=15)
    log.info("done — published %d weather observations to %s", sent, WEATHER_TOPIC)
    return 0


if __name__ == "__main__":
    sys.exit(main())
