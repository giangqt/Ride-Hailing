# Ride-Hailing Mobility Analysis

**Thesis:** Spatial–Temporal Analysis and Demand Forecasting of Ride-Hailing Mobility Using Open Trip Data
**Student:** Nguyen Truong Giang (23BI14139) · USTH Data Science
**Supervisor:** Nghiem Thi Phuong · ICT Lab · Hanoi, 2026

---

## Overview

An end-to-end streaming pipeline that ingests NYC TLC FHVHV open trip data, enriches events in real time, stores results in a time-series database, and feeds a live Grafana dashboard — paired with a Google Colab batch layer for spatial analysis, network analysis, anomaly detection, and demand forecasting.

| Metric | Value |
|---|---|
| Source dataset | NYC TLC FHVHV, April–September 2024 |
| Total trips (full dataset) | ~118 M |
| Replay window (loaded) | 30 days · 19.73 M trips |
| Taxi zones | 263 (NYC boroughs) |
| Dashboard panels | 22 |
| Git tag (current) | v0.6 (commit c1ad2a7) |

---

## Architecture

The system follows a six-stage pipeline. The streaming layer (Stages 1–3) runs inside Docker Compose on a local Ubuntu VM. The batch layer (Stage 4) runs in Google Colab. Storage and visualization (Stages 5–6) are served by the same Docker Compose stack.

```
Stage 1 ──► Stage 2 ──► Stage 3 ──► Stage 4 ──► Stage 5 ──► Stage 6
  Data       Kafka       Spark       Batch       Postgres    Grafana
Collection  Ingestion  Streaming   Analysis    TimescaleDB  Dashboard
(cron)     (KRaft)    (PySpark)   (Colab)     (14 tables)  (22 panels)
```

### Stage 1 — Data Collection

Python scripts + cron. No Kafka or Spark.

| Script | Schedule | Output |
|---|---|---|
| `scripts/download_zones.py` | one-time | `data/zones/` — shapefile, GeoJSON, centroids |
| `scripts/download_tlc.py` | monthly (`0 2 1 * *`) | `data/raw_trips/fhvhv_tripdata_YYYY-MM.parquet` |
| `scripts/fetch_weather.py` | hourly (`0 * * * *`) | `data/weather/hourly_weather.csv` + Kafka `weather-events` |
| `scripts/validate_data.py` | after download | `data/validation_reports/*.json` |

Weather is written to CSV unconditionally and additionally published to Kafka when `KAFKA_BROKERS` is set.
Historical weather (for the 30-day batch window) is backfilled from the Open-Meteo archive API by `scripts/backfill_weather.py`.

### Stage 2 — Kafka Ingestion

3-broker KRaft cluster. `scripts/replay_producer.py` reads the TLC parquet files and publishes to Kafka at configurable speed (1×–1000×, with time anchoring), simulating real-time ingestion.

**8 application topics:**

| Topic | Partitions | Retention | Producer | Consumer |
|---|---|---|---|---|
| `ride-events-raw` | 12 | 24 h | `replay_producer.py` | Spark Enrichment |
| `ride-events-enriched` | 12 | 48 h | Spark Enrichment | Spark Aggregation, Network |
| `ride-events-enriched-weather` | 12 | 48 h | Spark Enrichment | Spark Aggregation |
| `weather-events` | 3 | 24 h | `fetch_weather.py` | Spark Enrichment |
| `demand-per-zone` | 6 | 7 days | Spark Aggregation | Spark Forecasting, Grafana |
| `hotspot-alerts` | 3 | 24 h | Spark Aggregation | Grafana |
| `network-flow-updates` | 6 | 48 h | Spark Network | Grafana |
| `forecast-results` | 6 | 7 days | Spark Forecasting | Grafana |

### Stage 3 — Spark Structured Streaming

Master + 2 workers. Five PySpark jobs running micro-batches (10 s trigger).

| Job | File | Role |
|---|---|---|
| Enrichment | `spark/enrichment.py` | Zone broadcast join + stream-stream weather join (watermarked) |
| Aggregation | `spark/aggregation.py` | Windowed demand counts + hotspot rule |
| Network | `spark/network.py` | OD flow aggregation → `network-flow-updates` |
| Forecasting | `spark/forecasting.py` | Apply SARIMAX/ETS pickles per zone → `forecast-results` |
| PG Sink | `spark/pg_sink.py` | Idempotent ON CONFLICT upserts to PostgreSQL (4 topics, 1 job) |

Key engineering constraints documented during development:
- Watermarks must be placed on the literal join-key column, not on an upstream derived timestamp.
- ISO-8601-with-offset timestamps must be read as `StringType` then converted via `F.to_timestamp`; `TimestampType` in `from_json` returns silent NULLs.
- General window functions (rank, sum-over-partition) are rejected on streams — deferred to SQL on materialized tables or to Colab notebooks.
- The 14 GB VM sustains ~2 Spark JVMs concurrently; all four output topics are written by one sink job to stay within the memory budget.

### Stage 4 — Batch Analysis (Google Colab)

Eight notebooks. Each reads from PostgreSQL/TimescaleDB and writes figures + analysis tables back.

| Notebook | Analysis | DB tables written |
|---|---|---|
| `01` | Setup / connectivity check | — |
| `02` | Schema audit | — |
| `03_eda_analysis.ipynb` | Exploratory data analysis, weather correlations | — |
| `04_spatial_analysis.ipynb` | KDE heatmaps, PySAL Gi*/Moran's I/LISA | `spatial_statistics` |
| `05_network_analysis.ipynb` | NetworkX + Louvain communities, centrality | `network_centrality`, `network_flows` |
| `06_forecasting.ipynb` | SARIMAX + ETS training, evaluation | `forecast_results` |
| `07_temporal_analysis.ipynb` | ADTK anomaly detection + Prophet decomposition | `temporal_anomalies`, `trend_decomposition` |
| `08_hanoi_comparison.ipynb` | NYC vs Hanoi UTM mobility comparison | — |

Figures are persisted to Google Drive at `Colab Notebooks/ride_hailing_outputs/` and imported into the thesis.

### Stage 5 — Storage

PostgreSQL 16 + TimescaleDB 2.26.4. **14 tables total: 10 hypertables + 4 plain tables.**

**Hypertables (time-partitioned):**
`trip_events`, `hourly_demand`, `weather_data`, `hotspot_alerts`, `forecast_results`, `network_flows`, `network_centrality`, `spatial_statistics`, `temporal_anomalies`, `trend_decomposition`

**Plain tables:**
`taxi_zones`, `agg_duration`, `agg_hour_dow`, `agg_hourly_profile`

Continuous aggregates (`daily_demand`, `weekly_demand`) are defined on `hourly_demand`.

### Stage 6 — Visualization

Grafana 11.0.0. **22 panels** across Overview, Spatial, Temporal Patterns, Network Flows, Forecasting, and the automated-analysis trio (Gi*/LISA, ADTK anomalies, Prophet trend).

Dashboard JSON: `grafana/dashboards/ride_hailing_dashboard.json`

---

## Repository Layout

```
ride-hailing/
├── docker-compose.yml          # 11 services: Kafka ×3, Spark ×3, Postgres, TimescaleDB,
│                               #   Grafana, Schema Registry, Kafka UI
├── scripts/
│   ├── config.py               # Paths, URLs, thresholds — single source of truth
│   ├── logger.py               # Shared file + console logging
│   ├── download_zones.py       # One-time zone shapefile + GeoJSON + centroids
│   ├── download_tlc.py         # Monthly TLC parquet download (idempotent)
│   ├── fetch_weather.py        # Hourly OpenWeatherMap → CSV + Kafka
│   ├── validate_data.py        # Parquet integrity / null / range checks
│   ├── create_topics.py        # Idempotent Kafka topic creation
│   ├── replay_producer.py      # Parquet → Kafka at configurable speed
│   ├── batch_load_trips.py     # Resumable COPY loader (30-day bulk ingest)
│   ├── backfill_weather.py     # Open-Meteo archive → hourly_demand weather columns
│   └── forecast_demand.py      # Batch SARIMAX/ETS inference (params-only pickles)
├── spark/
│   ├── enrichment.py           # Zone join + weather stream-stream join
│   ├── aggregation.py          # Windowed demand + hotspot rule
│   ├── network.py              # OD flow aggregation
│   ├── forecasting.py          # Apply models → forecast-results topic
│   └── pg_sink.py              # 4-topic idempotent Postgres sink
├── notebooks/
│   ├── 03_eda_analysis.ipynb
│   ├── 04_spatial_analysis.ipynb
│   ├── 05_network_analysis.ipynb
│   ├── 06_forecasting.ipynb
│   ├── 07_temporal_analysis.ipynb
│   └── 08_hanoi_comparison.ipynb
├── models/                     # SARIMAX + ETS params-only pickles (14 + 4 files)
├── sql/
│   ├── create_tables.sql
│   ├── create_hypertables.sql
│   └── create_agg_tables.sql
├── grafana/
│   ├── dashboards/ride_hailing_dashboard.json
│   └── datasources/
├── data/
│   ├── raw_trips/              # fhvhv_tripdata_YYYY-MM.parquet
│   ├── zones/                  # shapefile, GeoJSON, lookup CSV, centroids CSV
│   ├── weather/                # hourly_weather.csv (append-only)
│   └── validation_reports/     # one JSON per validate_data.py run
└── crontab.txt
```

---

## Quick Start

### Prerequisites

- Docker + Docker Compose
- Python 3.10+
- Ubuntu 22/24 (tested on 14 GB RAM · 4 CPU · 98 GB disk VM)

### 1. Clone and configure

```bash
git clone https://github.com/giangqt/Ride-Hailing.git
cd ride-hailing
cp .env.example .env
# Edit .env — set OPENWEATHER_API_KEY and DB credentials
```

### 2. Download reference data (one-time)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/download_zones.py
```

### 3. Download trip data

```bash
# Downloads all available months (Apr–Sep 2024 for the thesis dataset)
python scripts/download_tlc.py --since 2024-01
python scripts/validate_data.py
```

### 4. Start the Docker stack

```bash
docker compose up -d
# Verify all 11 services are healthy
docker compose ps
```

### 5. Initialize Kafka topics and database schema

```bash
docker compose exec kafka-1 python /scripts/create_topics.py
psql -U rides -d rides -f sql/create_tables.sql
psql -U rides -d rides -f sql/create_hypertables.sql
psql -U rides -d rides -f sql/create_agg_tables.sql
```

### 6. Load trip data into PostgreSQL (batch path)

```bash
# Resumable — safe to interrupt and restart
python scripts/batch_load_trips.py
# Backfill historical weather from Open-Meteo archive
python scripts/backfill_weather.py
```

### 7. Start streaming (optional — for live replay)

```bash
# In tmux (recommended — survives terminal disconnects)
tmux new -s pipeline
# Window 1: PG sink
docker compose exec spark-master spark-submit spark/pg_sink.py
# Window 2: Enrichment + other jobs
docker compose exec spark-master spark-submit spark/enrichment.py
# Window 3: Replay producer
python scripts/replay_producer.py --speed 100 --loop
```

### 8. Run batch analysis (Colab)

Open each notebook in Google Colab in order (03 → 08). Set the `DB_URL` constant in Section 0 of each notebook to the PostgreSQL connection string (via ngrok tunnel if running on a local VM). Run all cells. Outputs are synced to Google Drive automatically.

### 9. Open the dashboard

Grafana is available at `http://localhost:3000` (default credentials: `admin`/`admin`).

---

## Infrastructure

| Component | Detail |
|---|---|
| Host | Ubuntu 24 VM · 14 GB RAM · 4 CPUs · 98 GB disk · VMware |
| Docker Compose | 11 services |
| Database | `host`: Docker Compose `postgres` service · database/user/password: `rides` |
| Project root | `~/ride-hailing/` |
| GitHub | `github.com/giangqt/Ride-Hailing` |
| JVM timezone | `Asia/Ho_Chi_Minh` (UTC+7) — known source of timestamp offset if not accounted for |

---

## Forecasting Models

Models are trained in Notebook 06 and serialized as **params-only pickles** (not full results objects).

- **SARIMAX(2,1,2)(1,1,1,s)** — 14 zone models (statsmodels)
- **ETS** — 4 zone models (statsmodels)
- Largest pickle: 53 KB (JFK, zone 132)
- Stored under `models/` — loaded at inference time by `spark/forecasting.py` and `scripts/forecast_demand.py`

> **Note:** Pickle only the lightweight dict `{order, seasonal_order, params}`, not the full `SARIMAXResults` object. Full results objects can reach hundreds of MB due to Kalman smoother state arrays.

---

## Spatial Analysis Notes

- **Gi* classification:** use `p_sim` alone for all confidence bands (90%/95%/99%); z-sign determines hot vs. cold. Do not mix permutation p-values with analytical z-score thresholds.
- **MAUP:** choropleth and KDE maps of the same data look different by design — zone-size effects and log-weighting account for the visual difference.
- **PySAL version:** Getis-Ord Gi* and Moran's I / LISA use the `esda` + `libpysal` stack.

---

## Known Constraints and Design Decisions

| Constraint | Decision |
|---|---|
| 14 GB VM memory | Single PG sink job writes 4 topics; max ~2 Spark JVMs concurrently |
| `foreachBatch` hangs after batch 0 on this VM | Documented environmental constraint; not a code bug |
| Streaming vs. batch tradeoff | For a replay dataset, batch yields the same analytical numbers; streaming demonstrates a production-shaped, generalizable architecture |
| TLC publication lag | ~2 months; `download_tlc.py` skips months newer than `today − 2 months` |
| Kafka retention | 48-hour retention on most topics; non-persistent volumes lose data on cluster restart |

---

## Cron Schedule

```
# Monthly TLC download
0 2 1 * *  /path/to/venv/bin/python /path/to/scripts/download_tlc.py

# Hourly weather fetch
0 * * * *  /path/to/venv/bin/python /path/to/scripts/fetch_weather.py

# Validation after download
15 2 1 * * /path/to/venv/bin/python /path/to/scripts/validate_data.py
```

See `crontab.txt` for the full schedule with logging paths.

---

## Validation Thresholds

Configured in `scripts/config.py`:

| Setting | Default | Meaning |
|---|---|---|
| `VALIDATION_MIN_ROWS` | 5,000,000 | Below this indicates a truncated file |
| `VALIDATION_MAX_NULL_FRAC` | 0.05 | Critical columns must be ≤ 5% null |
| `VALIDATION_ZONE_ID_MIN/MAX` | 1 / 265 | TLC LocationIDs (264/265 = "unknown") |

---

## Troubleshooting

**`OPENWEATHER_API_KEY is not set`** — Copy `.env.example` to `.env` and fill in the key. New OWM keys take ~10 minutes to activate.

**Parquet 404 from TLC** — The month is not yet published. TLC releases month M's data midway through month M+2. Check the [TLC release calendar](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

**`geopandas` import error** — Install geo extras: `pip install geopandas shapely pyproj`. On some Linux systems you also need `libgdal-dev` and `libgeos-dev` via `apt`.

**Validation fails with `row_count below threshold`** — Expected during testing on small subsets. Lower `VALIDATION_MIN_ROWS` in `config.py` while developing.

**Spark job hangs after batch 0** — Known VM memory constraint. Check swap usage with `free -h`. Reduce executor memory or kill other JVM processes before restarting.

**Kafka topics lost after restart** — Docker volumes are non-persistent by default. Re-run `create_topics.py` and restart the replay producer after `docker compose down -v`.

**Ngrok URL rotates on VM reboot** — Update the `DB_URL` constant in Section 0 of each Colab notebook before re-running.

---

## Status Reports

Phase-by-phase completion reports (PDF) are in the project root:

| File | Phase |
|---|---|
| `phase1_status_report.pdf` | Stage 1 — Data Collection |
| `phase2_status_report.pdf` | Stage 2 — Kafka Ingestion |
| `phase3_status_report.pdf` | Stage 3 — Spark Streaming |
| `phase4_status_report.pdf` | Stage 4 — Batch Analysis |
| `phase5_status_report.pdf` | Stage 5 — TimescaleDB tuning |
| `phase6_status_report.pdf` | Stage 6 — Grafana dashboard (22 panels, v0.6) |
| `phase7_status_report.pdf` | Consolidation & hardening — 30-day window, weather backfill, SARIMAX migration |