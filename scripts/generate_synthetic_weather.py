#!/usr/bin/env python3
"""
scripts/generate_synthetic_weather.py

Generate plausible synthetic hourly weather observations and publish them to
the weather-events Kafka topic.

When to use this:
  - Your hourly_weather.csv has only a handful of rows from cron runs
  - You're demoing the streaming pipeline and need weather for "right now"
  - You want reproducible weather for tests

Schema matches scripts/fetch_weather.py exactly so the join in
spark/enrichment_weather.py treats real and synthetic observations
identically:

    observation_time   ISO 8601 with UTC offset
    station_id         KNYC
    temperature_c      ~15-25°C with daily curve (cooler 4am, peak 3pm)
    precipitation_mm   None most hours; some rain in random windows
    wind_speed_ms      3-8 m/s with mild noise
    humidity_pct       50-80% inversely correlated with temperature
    weather_condition  Clear / Clouds / Rain (correlated with precipitation)

Examples
--------
Default — 48h of hourly observations centered on "now", publishes to Kafka::

    python scripts/generate_synthetic_weather.py

72h of data, anchored to a specific time::

    python scripts/generate_synthetic_weather.py --hours 72 --start "2026-05-01T00:00:00"

Dry run: print what we'd publish, without touching Kafka::

    python scripts/generate_synthetic_weather.py --dry-run

Append to the CSV instead of (or in addition to) Kafka::

    python scripts/generate_synthetic_weather.py --csv data/weather/synthetic.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Reuse Phase 1 config conventions
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import KAFKA_BROKERS  # noqa: E402
from logger import get_logger     # noqa: E402

log = get_logger("generate_synthetic_weather")

WEATHER_TOPIC = "weather-events"
STATION_ID = "KNYC"


# ---------------------------------------------------------------------------
# Plausible NYC weather model — daily temperature curve + correlated noise
# ---------------------------------------------------------------------------

def _temperature_at(hour_utc: int, base_c: float = 18.0, swing_c: float = 6.0) -> float:
    """Return temperature for a given UTC hour using a sinusoidal daily curve.

    NYC peak temperature is around 3pm local (=19:00 UTC during EDT).
    Trough is around 4am local (=08:00 UTC during EDT).
    """
    # Phase shifted so peak is at hour=19 UTC (3pm EDT), trough at 7 UTC (3am EDT)
    radians = 2 * math.pi * (hour_utc - 7) / 24
    return base_c + swing_c * math.sin(radians) + random.uniform(-0.8, 0.8)


def _humidity_from_temp(temp_c: float) -> float:
    """Inverse-ish relationship: cooler hours = higher humidity."""
    base = 100 - 1.5 * temp_c    # rough inverse
    return max(40.0, min(95.0, base + random.uniform(-5, 5)))


def _make_observation(observation_time: datetime, rain_window: bool) -> dict:
    """Build one weather observation matching fetch_weather.py's schema."""
    hour_utc = observation_time.hour
    temp_c = _temperature_at(hour_utc)
    humidity = _humidity_from_temp(temp_c)
    wind = random.uniform(3.0, 8.0)

    if rain_window:
        precipitation = round(random.uniform(0.5, 4.0), 2)
        condition = "Rain"
    elif random.random() < 0.35:    # 35% chance of clouds even without rain
        precipitation = None
        condition = "Clouds"
    else:
        precipitation = None
        condition = "Clear"

    return {
        "observation_time": observation_time.isoformat(),
        "station_id": STATION_ID,
        "temperature_c": round(temp_c, 2),
        "precipitation_mm": precipitation,
        "wind_speed_ms": round(wind, 2),
        "humidity_pct": round(humidity, 1),
        "weather_condition": condition,
    }


def _generate_rain_windows(start: datetime, hours: int) -> set[int]:
    """Pick a few hour offsets where it'll rain. Adds realism to the data."""
    n_windows = random.randint(1, 3)
    rain_offsets: set[int] = set()
    for _ in range(n_windows):
        window_start = random.randint(0, max(1, hours - 4))
        window_len = random.randint(2, 5)
        for h in range(window_start, min(hours, window_start + window_len)):
            rain_offsets.add(h)
    return rain_offsets


def generate(start: datetime, hours: int, seed: int | None = None) -> list[dict]:
    """Return ``hours`` synthetic observations starting at ``start`` (UTC)."""
    if seed is not None:
        random.seed(seed)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    rain_hours = _generate_rain_windows(start, hours)
    return [
        _make_observation(start + timedelta(hours=h), rain_window=(h in rain_hours))
        for h in range(hours)
    ]


# ---------------------------------------------------------------------------
# Sinks: Kafka + optional CSV
# ---------------------------------------------------------------------------

def publish_to_kafka(observations: list[dict], brokers: str) -> int:
    """Publish each observation to weather-events. Returns # successful."""
    try:
        from confluent_kafka import Producer
    except ImportError:
        log.error("confluent-kafka not installed — cannot publish to Kafka.")
        return 0

    delivered = {"count": 0, "errors": 0}

    def _on_delivery(err, msg):
        if err is not None:
            delivered["errors"] += 1
        else:
            delivered["count"] += 1

    producer = Producer({
        "bootstrap.servers": brokers,
        "client.id": "synthetic-weather",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
    })
    for obs in observations:
        producer.produce(
            topic=WEATHER_TOPIC,
            key=obs["station_id"].encode("utf-8"),
            value=json.dumps(obs).encode("utf-8"),
            on_delivery=_on_delivery,
        )
    producer.flush(timeout=30)

    if delivered["errors"]:
        log.warning("%d delivery errors out of %d", delivered["errors"], len(observations))
    return delivered["count"]


def append_to_csv(observations: list[dict], path: Path) -> int:
    """Append observations to CSV (creating with header if needed)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(observations[0].keys()))
        if write_header:
            writer.writeheader()
        writer.writerows(observations)
    return len(observations)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--hours", type=int, default=48,
                   help="how many hours of observations to generate. Default 48.")
    p.add_argument("--start", default=None,
                   help="ISO timestamp to start from (UTC). Default: 24h before now, "
                        "so 'now' falls in the middle of the generated window.")
    p.add_argument("--seed", type=int, default=None,
                   help="RNG seed for reproducible output. Default: nondeterministic.")
    p.add_argument("--dry-run", action="store_true",
                   help="print observations to stdout, don't publish to Kafka.")
    p.add_argument("--csv", type=Path, default=None,
                   help="also append observations to this CSV file.")
    p.add_argument("--no-kafka", action="store_true",
                   help="skip the Kafka publish step (use with --csv).")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()

    # Default start: 24h ago, so generated data brackets "now"
    if args.start is None:
        start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) \
                                          - timedelta(hours=args.hours // 2)
    else:
        start = datetime.fromisoformat(args.start)
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

    log.info("generating %d hourly observations starting %s",
             args.hours, start.isoformat())
    observations = generate(start, args.hours, seed=args.seed)
    log.info("generated %d observations", len(observations))

    if args.dry_run:
        for obs in observations[:5]:
            print(json.dumps(obs))
        if len(observations) > 5:
            print(f"... ({len(observations) - 5} more, omitted)")
        return 0

    if args.csv:
        n = append_to_csv(observations, args.csv)
        log.info("appended %d rows to %s", n, args.csv)

    if not args.no_kafka:
        if not KAFKA_BROKERS:
            log.error("KAFKA_BROKERS not set; cannot publish to Kafka. "
                      "Use --dry-run or --csv if Kafka isn't available.")
            return 2
        n = publish_to_kafka(observations, KAFKA_BROKERS)
        log.info("published %d observations to Kafka topic '%s'",
                 n, WEATHER_TOPIC)

    return 0


if __name__ == "__main__":
    sys.exit(main())
