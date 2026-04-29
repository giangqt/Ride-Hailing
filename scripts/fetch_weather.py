#!/usr/bin/env python3
"""
fetch_weather.py
================

Hourly weather fetch for the NYC reference station (default: Central Park /
KNYC) from OpenWeatherMap's Current Weather Data API.

Outputs:
    1. data/weather/hourly_weather.csv           (always - append + dedup)
    2. Kafka topic 'weather-events'              (only if KAFKA_BROKERS is set)

The CSV is the source of truth in Phase 1 (before Kafka is up). Once Stage 2
(Kafka) is online, the same script also publishes to the weather-events
topic; the CSV continues to act as a durable backup.

CSV schema matches the weather_data hypertable in the Stage 5 schema:
    observation_time, temperature_c, precipitation_mm, wind_speed_ms,
    humidity_pct, weather_condition, station_id

Dedup rule: if the rounded-to-hour observation_time already exists in the
CSV for the same station_id, we skip the write. This makes the script safe
to re-run inside the same hour.

Cron schedule (per spec):  0 * * * *   (top of every hour)
Pipeline stage:            Stage 1 (Data Collection)
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

from config import (
    KAFKA_BROKERS,
    KAFKA_WEATHER_TOPIC,
    OPENWEATHER_API_KEY,
    OPENWEATHER_URL,
    WEATHER_CSV_PATH,
    WEATHER_LAT,
    WEATHER_LON,
    WEATHER_STATION_ID,
)
from logger import get_logger

log = get_logger("fetch_weather")

CSV_FIELDS = [
    "observation_time",
    "station_id",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_ms",
    "humidity_pct",
    "weather_condition",
]


# ---------------------------------------------------------------------------
# OpenWeatherMap call
# ---------------------------------------------------------------------------
def _fetch_openweather(lat: float, lon: float, api_key: str) -> dict:
    params = {"lat": lat, "lon": lon, "appid": api_key, "units": "metric"}
    log.info("GET %s lat=%s lon=%s", OPENWEATHER_URL, lat, lon)
    r = requests.get(OPENWEATHER_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _parse_owm_payload(payload: dict, station_id: str) -> dict:
    """Map the OWM JSON into our weather_data record schema."""
    main = payload.get("main", {})
    wind = payload.get("wind", {})
    rain = payload.get("rain", {}) or {}
    snow = payload.get("snow", {}) or {}
    weather = (payload.get("weather") or [{}])[0]

    # OWM precip is reported as mm in the last 1h or 3h. Sum rain+snow at the
    # finest available granularity.
    precip_mm: Optional[float] = None
    for key in ("1h", "3h"):
        if key in rain or key in snow:
            precip_mm = float(rain.get(key, 0.0)) + float(snow.get(key, 0.0))
            break

    # OWM 'dt' is unix UTC seconds. Round to the hour for hypertable alignment.
    obs_dt_utc = datetime.fromtimestamp(payload.get("dt", 0), tz=timezone.utc)
    obs_dt_hour = obs_dt_utc.replace(minute=0, second=0, microsecond=0)

    return {
        "observation_time": obs_dt_hour.isoformat(),
        "station_id": station_id,
        "temperature_c": float(main.get("temp")) if main.get("temp") is not None else None,
        "precipitation_mm": precip_mm,
        "wind_speed_ms": float(wind.get("speed")) if wind.get("speed") is not None else None,
        "humidity_pct": float(main.get("humidity")) if main.get("humidity") is not None else None,
        "weather_condition": weather.get("main"),  # 'Clear', 'Rain', 'Snow', ...
    }


# ---------------------------------------------------------------------------
# CSV append + dedup
# ---------------------------------------------------------------------------
def _csv_already_has(path: Path, observation_time: str, station_id: str) -> bool:
    if not path.exists():
        return False
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("observation_time") == observation_time
                    and row.get("station_id") == station_id):
                return True
    return False


def _csv_append(path: Path, record: dict) -> None:
    new_file = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow({k: record.get(k) for k in CSV_FIELDS})


# ---------------------------------------------------------------------------
# Optional Kafka publish (skipped silently if KAFKA_BROKERS is empty or
# the kafka client isn't installed - useful in Phase 1 before Stage 2 is up).
# ---------------------------------------------------------------------------
def _publish_to_kafka(record: dict) -> bool:
    if not KAFKA_BROKERS:
        log.debug("KAFKA_BROKERS not set - skipping Kafka publish.")
        return False
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError:
        log.warning("kafka-python not installed - skipping Kafka publish.")
        return False

    try:
        producer = KafkaProducer(
            bootstrap_servers=[b.strip() for b in KAFKA_BROKERS.split(",")],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
            acks="all",
            retries=3,
            request_timeout_ms=10_000,
        )
        future = producer.send(
            KAFKA_WEATHER_TOPIC,
            key=record["station_id"],
            value=record,
        )
        future.get(timeout=10)
        producer.flush()
        producer.close()
        log.info("Published to Kafka topic %s", KAFKA_WEATHER_TOPIC)
        return True
    except Exception as e:
        log.error("Kafka publish failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--lat", type=float, default=WEATHER_LAT,
                        help=f"Latitude (default: {WEATHER_LAT})")
    parser.add_argument("--lon", type=float, default=WEATHER_LON,
                        help=f"Longitude (default: {WEATHER_LON})")
    parser.add_argument("--station-id", default=WEATHER_STATION_ID,
                        help=f"Logical station ID (default: {WEATHER_STATION_ID})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + parse but don't write CSV or publish")
    args = parser.parse_args()

    if not OPENWEATHER_API_KEY:
        log.error("OPENWEATHER_API_KEY is not set. "
                  "Add it to .env or export it before running.")
        return 2

    try:
        payload = _fetch_openweather(args.lat, args.lon, OPENWEATHER_API_KEY)
    except requests.HTTPError as e:
        log.error("OpenWeatherMap HTTP error: %s", e)
        return 1
    except requests.RequestException as e:
        log.error("OpenWeatherMap request failed: %s", e)
        return 1

    record = _parse_owm_payload(payload, args.station_id)
    log.info("Observation: time=%s temp=%.1f°C precip=%s humidity=%s%% cond=%s",
             record["observation_time"],
             record["temperature_c"] if record["temperature_c"] is not None else float("nan"),
             record["precipitation_mm"],
             record["humidity_pct"],
             record["weather_condition"])

    if args.dry_run:
        log.info("[dry-run] not writing CSV or publishing to Kafka")
        return 0

    # CSV: append unless this hour+station is already there.
    if _csv_already_has(WEATHER_CSV_PATH, record["observation_time"],
                        record["station_id"]):
        log.info("CSV already has %s @ %s - skipping CSV append.",
                 record["station_id"], record["observation_time"])
    else:
        _csv_append(WEATHER_CSV_PATH, record)
        log.info("Appended observation to %s", WEATHER_CSV_PATH.name)

    # Kafka publish: best-effort.
    _publish_to_kafka(record)

    return 0


if __name__ == "__main__":
    sys.exit(main())
