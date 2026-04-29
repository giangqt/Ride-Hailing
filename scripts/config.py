"""
Shared configuration for Phase 1 data-collection scripts.

All paths, URLs, and tuning constants live here. Scripts import from this
module rather than hard-coding values, so the project has a single source
of truth.

Environment variables (loaded from .env if present):
    DATA_DIR             - base data directory (default: ./data)
    LOG_DIR              - log directory (default: ./logs)
    OPENWEATHER_API_KEY  - required by fetch_weather.py
    WEATHER_LAT          - default Central Park, NY
    WEATHER_LON
    WEATHER_STATION_ID   - logical station identifier used as Kafka key
    KAFKA_BROKERS        - optional, e.g. "localhost:9092"; if unset, weather
                           is only written to CSV (useful in Phase 1 before
                           Kafka is up)
"""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Optional .env loader. We don't hard-require python-dotenv because the
# scripts must still run on a bare cron environment.
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore

    # Look for .env in the project root (parent of the scripts/ dir).
    _ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
    if _ENV_PATH.exists():
        load_dotenv(_ENV_PATH)
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Project layout
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
LOG_DIR = Path(os.getenv("LOG_DIR", PROJECT_ROOT / "logs"))

RAW_TRIPS_DIR = DATA_DIR / "raw_trips"
ZONES_DIR = DATA_DIR / "zones"
WEATHER_DIR = DATA_DIR / "weather"
VALIDATION_DIR = DATA_DIR / "validation_reports"

for _d in (RAW_TRIPS_DIR, ZONES_DIR, WEATHER_DIR, VALIDATION_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# NYC TLC sources
#
# The TLC publishes FHVHV (Uber/Lyft) trip records monthly as Parquet on
# CloudFront. The S3 bucket s3://nyctlc/ is the underlying storage but the
# HTTPS endpoint below is the documented public URL.
# ---------------------------------------------------------------------------
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_FILENAME_TEMPLATE = "fhvhv_tripdata_{year:04d}-{month:02d}.parquet"

# Earliest month available for FHVHV (Uber/Lyft). TLC began publishing this
# subset in Feb 2019.
TLC_FHVHV_FIRST_MONTH = (2019, 2)

# Publication delay: TLC typically releases month M's data midway through
# month M+2. We refuse to attempt downloads that are unlikely to exist yet.
TLC_PUBLICATION_LAG_MONTHS = 2

# Taxi-zone reference data (used by download_zones.py).
TLC_ZONES_SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
TLC_ZONES_LOOKUP_CSV_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)


# ---------------------------------------------------------------------------
# Weather (NOAA / OpenWeatherMap)
# ---------------------------------------------------------------------------
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"

# Central Park (KNYC) - the canonical NYC weather reference site.
WEATHER_LAT = float(os.getenv("WEATHER_LAT", "40.7794"))
WEATHER_LON = float(os.getenv("WEATHER_LON", "-73.9692"))
WEATHER_STATION_ID = os.getenv("WEATHER_STATION_ID", "KNYC")

WEATHER_CSV_PATH = WEATHER_DIR / "hourly_weather.csv"


# ---------------------------------------------------------------------------
# Kafka (optional in Phase 1; used once Stage 2 is up)
# ---------------------------------------------------------------------------
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "")  # empty string = disabled
KAFKA_WEATHER_TOPIC = os.getenv("KAFKA_WEATHER_TOPIC", "weather-events")


# ---------------------------------------------------------------------------
# Validation thresholds (per spec - tuned for FHVHV monthly files)
# ---------------------------------------------------------------------------
# A typical FHVHV month has 18-22M rows. Anything well below 5M signals a
# truncated or wrong-month file.
VALIDATION_MIN_ROWS = 5_000_000

# Columns that must exist and have low null rate for the file to be usable
# downstream by Spark Enrichment.
VALIDATION_REQUIRED_COLS = (
    "hvfhs_license_num",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_time",
)

# Critical columns must have null fraction below this threshold.
VALIDATION_MAX_NULL_FRAC = 0.05
VALIDATION_CRITICAL_COLS = (
    "pickup_datetime",
    "PULocationID",
    "DOLocationID",
)

# PULocationID / DOLocationID must lie in [1, 265]. (TLC reserves IDs 264 &
# 265 for "unknown" / "outside NYC", but they are valid values.)
VALIDATION_ZONE_ID_MIN = 1
VALIDATION_ZONE_ID_MAX = 265
