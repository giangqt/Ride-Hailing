-- =====================================================================
-- create_tables.sql  ·  Phase 5 Block 1  ·  canonical schema (11 tables)
-- Reconciled against the live `rides` database on 2026-06-08.
-- Idempotent: safe to re-run. Tables only; hypertables/indexes/policies
-- live in create_hypertables.sql and the later Phase 5 blocks.
--
-- Reality notes (differ from the spec PDF, captured here as ground truth):
--   * PostGIS is NOT installed; taxi_zones stores geometry as centroid
--     lat/lon floats, not a GEOMETRY column.
--   * Network model is two tables, not one: network_flows = OD edges,
--     network_centrality = per-zone centrality (pagerank/community incl.).
--   * Every table carries a bigint surrogate id alongside its natural key.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Reference table (static, 265 rows incl. TLC sentinels 264/265)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taxi_zones (
    zone_id      INTEGER PRIMARY KEY,
    zone_name    VARCHAR(100),
    borough      VARCHAR(50),
    centroid_lat DOUBLE PRECISION,
    centroid_lon DOUBLE PRECISION
);

-- ---------------------------------------------------------------------
-- Core pipeline tables (written by Spark PG sink)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trip_events (
    id               BIGSERIAL,
    pickup_datetime  TIMESTAMPTZ NOT NULL,
    dropoff_datetime TIMESTAMPTZ NOT NULL,
    pu_zone_id       INTEGER NOT NULL,
    do_zone_id       INTEGER NOT NULL,
    trip_miles       DOUBLE PRECISION,
    trip_time_min    DOUBLE PRECISION,
    hour_of_day      SMALLINT,
    day_of_week      SMALLINT,
    is_weekend       BOOLEAN,
    is_rush_hour     BOOLEAN
);

CREATE TABLE IF NOT EXISTS hourly_demand (
    id               BIGSERIAL,
    time_bucket      TIMESTAMPTZ NOT NULL,
    zone_id          INTEGER NOT NULL,
    pickup_count     INTEGER NOT NULL,
    dropoff_count    INTEGER NOT NULL,
    avg_trip_miles   DOUBLE PRECISION,
    avg_trip_time    DOUBLE PRECISION,
    avg_temperature  DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS weather_data (
    id                BIGSERIAL,
    observation_time  TIMESTAMPTZ NOT NULL,
    station_id        VARCHAR(20),
    temperature_c     DOUBLE PRECISION,
    precipitation_mm  DOUBLE PRECISION,
    wind_speed_ms     DOUBLE PRECISION,
    humidity_pct      DOUBLE PRECISION,
    weather_condition VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS hotspot_alerts (
    id              BIGSERIAL,
    detected_at     TIMESTAMPTZ NOT NULL,
    zone_id         INTEGER NOT NULL,
    demand_current  INTEGER NOT NULL,
    demand_baseline DOUBLE PRECISION NOT NULL,
    ratio           DOUBLE PRECISION,
    severity        VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS forecast_results (
    id               BIGSERIAL,
    forecast_time    TIMESTAMPTZ NOT NULL,
    zone_id          INTEGER,
    predicted_demand DOUBLE PRECISION,
    lower_bound      DOUBLE PRECISION,
    upper_bound      DOUBLE PRECISION,
    model_name       VARCHAR(30),
    mae              DOUBLE PRECISION
);

-- ---------------------------------------------------------------------
-- Network tables (two-table model: edges + per-zone centrality)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS network_flows (
    id             BIGSERIAL,
    time_window    TIMESTAMPTZ NOT NULL,
    origin_zone_id INTEGER NOT NULL,
    dest_zone_id   INTEGER NOT NULL,
    trip_count     INTEGER NOT NULL,
    in_degree      DOUBLE PRECISION,
    out_degree     DOUBLE PRECISION,
    betweenness    DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS network_centrality (
    id            BIGSERIAL,
    analysis_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    zone_id       INTEGER,
    in_degree     BIGINT,
    out_degree    BIGINT,
    betweenness   DOUBLE PRECISION,
    pagerank      DOUBLE PRECISION,
    community     INTEGER
);

-- ---------------------------------------------------------------------
-- Automated analysis tables (written by Colab notebooks 04 + 07)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spatial_statistics (
    id                     BIGSERIAL,
    analysis_time          TIMESTAMPTZ NOT NULL DEFAULT now(),
    zone_id                INTEGER,
    gi_star_z              DOUBLE PRECISION,
    gi_star_p              DOUBLE PRECISION,
    moran_i                DOUBLE PRECISION,
    lisa_cluster           VARCHAR(20),
    is_significant_hotspot BOOLEAN
);

CREATE TABLE IF NOT EXISTS temporal_anomalies (
    id             BIGSERIAL,
    detected_at    TIMESTAMPTZ NOT NULL,
    zone_id        INTEGER,
    anomaly_type   VARCHAR(30),
    anomaly_score  DOUBLE PRECISION,
    demand_value   DOUBLE PRECISION,
    expected_value DOUBLE PRECISION,
    is_anomaly     BOOLEAN
);

CREATE TABLE IF NOT EXISTS trend_decomposition (
    id                   BIGSERIAL,
    analysis_time        TIMESTAMPTZ NOT NULL,
    zone_id              INTEGER,
    trend_value          DOUBLE PRECISION,
    seasonal_daily       DOUBLE PRECISION,
    seasonal_weekly      DOUBLE PRECISION,
    changepoint_detected BOOLEAN,
    holiday_effect       DOUBLE PRECISION,
    residual             DOUBLE PRECISION
);

-- ---------------------------------------------------------------------
-- Foreign keys (all -> taxi_zones(zone_id), NO ACTION).
-- Guarded so re-running never errors on an existing constraint.
-- ---------------------------------------------------------------------
DO $$
DECLARE
    fk RECORD;
BEGIN
    FOR fk IN
        SELECT * FROM (VALUES
            ('trip_events',         'trip_events_pu_zone_id_fkey',       'pu_zone_id'),
            ('trip_events',         'trip_events_do_zone_id_fkey',       'do_zone_id'),
            ('hourly_demand',       'hourly_demand_zone_id_fkey',        'zone_id'),
            ('hotspot_alerts',      'hotspot_alerts_zone_id_fkey',       'zone_id'),
            ('forecast_results',    'forecast_results_zone_id_fkey',     'zone_id'),
            ('network_flows',       'network_flows_origin_zone_id_fkey', 'origin_zone_id'),
            ('network_flows',       'network_flows_dest_zone_id_fkey',   'dest_zone_id'),
            ('network_centrality',  'network_centrality_zone_id_fkey',   'zone_id'),
            ('spatial_statistics',  'spatial_statistics_zone_id_fkey',   'zone_id'),
            ('temporal_anomalies',  'temporal_anomalies_zone_id_fkey',   'zone_id'),
            ('trend_decomposition', 'trend_decomposition_zone_id_fkey',  'zone_id')
        ) AS t(tbl, cname, col)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = fk.cname
        ) THEN
            EXECUTE format(
                'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES taxi_zones(zone_id)',
                fk.tbl, fk.cname, fk.col
            );
        END IF;
    END LOOP;
END $$;