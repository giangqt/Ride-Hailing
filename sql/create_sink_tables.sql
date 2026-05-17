-- Phase 3 Block 6 sink tables.
-- Run order: this file is idempotent (CREATE IF NOT EXISTS), can be re-run safely.
-- All 4 tables are TimescaleDB hypertables partitioned by their primary time column.
-- FK to taxi_zones (which must already exist).

-- ============================================================================
-- trip_events: one row per FHVHV trip, populated from ride-events-enriched.
--   Upsert key: (pickup_datetime, pu_zone_id, do_zone_id, dropoff_datetime)
--   (Block 2's enrichment.py does NOT emit a stable trip_id; this composite
--   natural key is the next-best identity. Collisions theoretically possible
--   for two trips at the exact same second between the same zone pair, but
--   negligible at NYC FHVHV scale.)
-- ============================================================================

CREATE TABLE IF NOT EXISTS trip_events (
    id                BIGSERIAL,
    pickup_datetime   TIMESTAMPTZ NOT NULL,
    dropoff_datetime  TIMESTAMPTZ NOT NULL,
    pu_zone_id        INTEGER NOT NULL REFERENCES taxi_zones(zone_id),
    do_zone_id        INTEGER NOT NULL REFERENCES taxi_zones(zone_id),
    trip_miles        DOUBLE PRECISION,
    trip_time_min     DOUBLE PRECISION,
    hour_of_day       SMALLINT,
    day_of_week       SMALLINT,
    is_weekend        BOOLEAN,
    is_rush_hour      BOOLEAN,
    PRIMARY KEY (pickup_datetime, pu_zone_id, do_zone_id, dropoff_datetime)
);

SELECT create_hypertable(
    'trip_events', 'pickup_datetime',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_trip_events_pu_zone
    ON trip_events (pu_zone_id, pickup_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_trip_events_do_zone
    ON trip_events (do_zone_id, pickup_datetime DESC);


-- ============================================================================
-- hourly_demand: aggregated per (15-min window, zone), populated from demand-per-zone.
--   Upsert key: (time_bucket, zone_id)
-- ============================================================================

CREATE TABLE IF NOT EXISTS hourly_demand (
    id                BIGSERIAL,
    time_bucket       TIMESTAMPTZ NOT NULL,
    zone_id           INTEGER NOT NULL REFERENCES taxi_zones(zone_id),
    pickup_count      INTEGER NOT NULL,
    dropoff_count     INTEGER NOT NULL,
    avg_trip_miles    DOUBLE PRECISION,
    avg_trip_time     DOUBLE PRECISION,
    avg_temperature   DOUBLE PRECISION,
    precipitation_mm  DOUBLE PRECISION,
    PRIMARY KEY (time_bucket, zone_id)
);

SELECT create_hypertable(
    'hourly_demand', 'time_bucket',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_hourly_demand_zone
    ON hourly_demand (zone_id, time_bucket DESC);


-- ============================================================================
-- hotspot_alerts: triggered when demand > 2x baseline, populated from hotspot-alerts.
--   Upsert key: (detected_at, zone_id)
-- ============================================================================

CREATE TABLE IF NOT EXISTS hotspot_alerts (
    id                BIGSERIAL,
    detected_at       TIMESTAMPTZ NOT NULL,
    zone_id           INTEGER NOT NULL REFERENCES taxi_zones(zone_id),
    demand_current    INTEGER NOT NULL,
    demand_baseline   DOUBLE PRECISION NOT NULL,
    ratio             DOUBLE PRECISION,
    severity          VARCHAR(20) NOT NULL,
    PRIMARY KEY (detected_at, zone_id)
);

SELECT create_hypertable(
    'hotspot_alerts', 'detected_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_hotspot_alerts_severity
    ON hotspot_alerts (severity, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_hotspot_alerts_zone
    ON hotspot_alerts (zone_id, detected_at DESC);


-- ============================================================================
-- network_flows: OD pair flow counts, populated from network-flow-updates.
--   Upsert key: (time_window, origin_zone_id, dest_zone_id)
--
--   in_degree / out_degree: computed by PG Sink via SQL window functions at
--   INSERT time. The Spark streaming job emits only the 4 streaming fields;
--   degrees are derived here.
--
--   betweenness: populated by Phase 4 Colab (NetworkX), nullable here.
-- ============================================================================

CREATE TABLE IF NOT EXISTS network_flows (
    id              BIGSERIAL,
    time_window     TIMESTAMPTZ NOT NULL,
    origin_zone_id  INTEGER NOT NULL REFERENCES taxi_zones(zone_id),
    dest_zone_id    INTEGER NOT NULL REFERENCES taxi_zones(zone_id),
    trip_count      INTEGER NOT NULL,
    in_degree       DOUBLE PRECISION,
    out_degree      DOUBLE PRECISION,
    betweenness     DOUBLE PRECISION,
    PRIMARY KEY (time_window, origin_zone_id, dest_zone_id)
);

SELECT create_hypertable(
    'network_flows', 'time_window',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_network_flows_origin
    ON network_flows (origin_zone_id, time_window DESC);
CREATE INDEX IF NOT EXISTS idx_network_flows_dest
    ON network_flows (dest_zone_id, time_window DESC);