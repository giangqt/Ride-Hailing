-- =====================================================================
-- create_continuous_aggregates.sql  ·  Phase 5 Block 4
-- Two continuous aggregates over hourly_demand, kept per-zone so the
-- spatial dimension survives (Grafana borough/zone filters need zone_id).
--
-- IMPORTANT: do NOT run this file with psql -1 / --single-transaction.
-- A continuous aggregate cannot be created inside a transaction block;
-- piped statements must auto-commit individually (psql default).
--
-- Idempotent: IF NOT EXISTS on the views; refresh policies guarded
-- against timescaledb_information.jobs so re-runs do not error.
-- =====================================================================

-- ---- daily_demand_summary --------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_demand_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 day', time_bucket) AS bucket,
    zone_id,
    SUM(pickup_count)      AS total_pickups,
    SUM(dropoff_count)     AS total_dropoffs,
    AVG(avg_trip_miles)    AS avg_trip_miles,
    AVG(avg_trip_time)     AS avg_trip_time,
    AVG(avg_temperature)   AS avg_temperature,
    SUM(precipitation_mm)  AS total_precipitation_mm
FROM hourly_demand
GROUP BY bucket, zone_id
WITH NO DATA;

-- ---- weekly_demand_summary -------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS weekly_demand_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '7 days', time_bucket) AS bucket,
    zone_id,
    SUM(pickup_count)      AS total_pickups,
    SUM(dropoff_count)     AS total_dropoffs,
    AVG(avg_trip_miles)    AS avg_trip_miles,
    AVG(avg_trip_time)     AS avg_trip_time,
    AVG(avg_temperature)   AS avg_temperature,
    SUM(precipitation_mm)  AS total_precipitation_mm
FROM hourly_demand
GROUP BY bucket, zone_id
WITH NO DATA;

-- ---- refresh policies (guarded) --------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.jobs
        WHERE application_name LIKE 'Refresh Continuous Aggregate Policy%'
          AND hypertable_name = (
              SELECT materialization_hypertable_name
              FROM timescaledb_information.continuous_aggregates
              WHERE view_name = 'daily_demand_summary')
    ) THEN
        PERFORM add_continuous_aggregate_policy('daily_demand_summary',
            start_offset => INTERVAL '3 days',
            end_offset   => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour');
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.jobs
        WHERE application_name LIKE 'Refresh Continuous Aggregate Policy%'
          AND hypertable_name = (
              SELECT materialization_hypertable_name
              FROM timescaledb_information.continuous_aggregates
              WHERE view_name = 'weekly_demand_summary')
    ) THEN
        PERFORM add_continuous_aggregate_policy('weekly_demand_summary',
            start_offset => INTERVAL '21 days',
            end_offset   => INTERVAL '1 hour',
            schedule_interval => INTERVAL '6 hours');
    END IF;
END $$;

-- ---- initial materialization of existing data ------------------------
-- WITH NO DATA above means the views start empty; backfill once here so
-- Grafana has data immediately. NULL window = all data.
CALL refresh_continuous_aggregate('daily_demand_summary',  NULL, NULL);
CALL refresh_continuous_aggregate('weekly_demand_summary', NULL, NULL);