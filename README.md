# Phase 1 — Data Collection

Stage 1 of the pipeline: downloads raw NYC TLC trip data, NYC taxi zones,
and hourly weather. Validates every download. No Kafka or Spark yet.

## What this phase produces

```
data/
├── raw_trips/                 fhvhv_tripdata_YYYY-MM.parquet  (one per month)
├── zones/
│   ├── taxi_zones.shp         original shapefile (NAD83 / NY State Plane)
│   ├── taxi_zones.geojson     WGS84, used by Spark Enrichment broadcast join
│   ├── taxi_zone_lookup.csv   id -> name, borough
│   └── taxi_zone_centroids.csv  zone_id, name, borough, lat, lon
├── weather/
│   └── hourly_weather.csv     append-only, hourly observations
└── validation_reports/        one JSON per validate_data.py run
```

## Quick start

```bash
# 1. Install Phase 1 dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env: at minimum, set OPENWEATHER_API_KEY

# 3. One-time: download taxi zones (takes ~30 seconds)
python scripts/download_zones.py

# 4. Backfill historical trip data (large download - 18-22M rows/month)
python scripts/download_tlc.py --since 2024-01

# 5. Validate everything that was downloaded
python scripts/validate_data.py

# 6. Test weather fetch
python scripts/fetch_weather.py --dry-run     # confirm API key works
python scripts/fetch_weather.py               # write first row to CSV

# 7. Smoke-test the code
python tests/test_smoke.py
```

## File map

| File                          | Purpose                                          | Schedule (cron) |
| ----------------------------- | ------------------------------------------------ | --------------- |
| `scripts/config.py`           | Single source of truth: paths, URLs, thresholds  | (imported)      |
| `scripts/logger.py`           | Shared file + console logging                    | (imported)      |
| `scripts/download_zones.py`   | One-time TLC zone shapefile + GeoJSON + centroids| one-time        |
| `scripts/download_tlc.py`     | Monthly FHVHV parquet downloader, idempotent     | `0 2 1 * *`     |
| `scripts/fetch_weather.py`    | Hourly OpenWeatherMap fetch -> CSV (+ optional Kafka) | `0 * * * *`|
| `scripts/validate_data.py`    | Parquet integrity / null / range / date checks   | after download  |
| `crontab.txt`                 | Sample cron schedule per spec                    | -               |
| `tests/test_smoke.py`         | Offline smoke tests                              | -               |

## How the four scripts cooperate

```
                           one-time
                            ┌──────────────────────┐
                            │ download_zones.py    │ ──► data/zones/*.geojson
                            └──────────────────────┘            │
                                                                ▼
   monthly cron                                     (used by Stage 3 Spark
   ┌──────────────────┐    success    ┌────────────────────────┐
   │ download_tlc.py  │──────────────►│ validate_data.py       │
   └──────────────────┘               │  --latest              │
        │                             └────────────────────────┘
        ▼                                       │
   data/raw_trips/*.parquet           data/validation_reports/*.json
                                                │
                                  exit code propagated to cron
                                  (non-zero => mail / alert)

   hourly cron
   ┌──────────────────┐
   │ fetch_weather.py │──► data/weather/hourly_weather.csv
   └──────────────────┘    [+ kafka topic 'weather-events' once Stage 2 is up]
```

## Design notes

**Why no Airflow?** Per the project's recent decision, simple cron + Python
scripts replace Airflow for Stage 1. Each script is self-contained,
idempotent, and uses non-zero exit codes for cron's MAILTO alerting.

**Why TLC is hosted on CloudFront, not S3.** The spec mentions `s3://nyctlc/`
as the underlying storage; the public HTTPS endpoint is the documented
download URL and what we use:
`https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_YYYY-MM.parquet`

**Why store weather as CSV in Phase 1.** Kafka isn't up until Stage 2.
`fetch_weather.py` writes CSV unconditionally and additionally publishes to
Kafka iff `KAFKA_BROKERS` is set. This keeps the script useful both before
and after Stage 2 goes live.

**Why TLC publication lag is 2 months.** TLC typically releases month M's
data midway through month M+2. `download_tlc.py` won't try months newer
than `today - TLC_PUBLICATION_LAG_MONTHS` to avoid spurious 404s.

## Validation thresholds (in `config.py`)

| Setting                     | Default       | Meaning                                  |
| --------------------------- | ------------- | ---------------------------------------- |
| `VALIDATION_MIN_ROWS`       | 5,000,000     | Below this looks like a truncated file   |
| `VALIDATION_MAX_NULL_FRAC`  | 0.05          | Critical cols must be ≤5% null           |
| `VALIDATION_ZONE_ID_MIN/MAX`| 1 / 265       | TLC LocationIDs (264/265 = "unknown")    |

Adjust these before running validation if you ingest the older Yellow/Green
TLC datasets — those have different baselines.

## Troubleshooting

- **`OPENWEATHER_API_KEY is not set`**: copy `.env.example` to `.env` and fill
  in your key. New OWM keys can take ~10 minutes to activate.
- **Parquet 404 from TLC**: that month is not yet published. Check
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page for the
  current release calendar.
- **`geopandas` import error in `download_zones.py`**: install the geo
  extras: `pip install geopandas shapely pyproj`. On some Linux distros
  you'll also need the system packages `libgdal-dev` and `libgeos-dev`.
- **Validation fails with "row_count below threshold"**: this is by design
  during testing on small subsets. Lower `VALIDATION_MIN_ROWS` in
  `config.py` while developing.

## Next: Phase 2

Phase 2 (Stage 2 of the pipeline) will spin up the Kafka cluster via
Docker Compose and add `replay_producer.py`, which reads these parquet
files and publishes trip events to `ride-events-raw`. The weather
publishing path in `fetch_weather.py` activates automatically once
`KAFKA_BROKERS` is set in `.env`.
