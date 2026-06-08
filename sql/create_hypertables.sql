-- =====================================================================
-- create_hypertables.sql  ·  Phase 5 Block 2  ·  TimescaleDB hypertables
-- Reconciled against the live `rides` database on 2026-06-08.
-- Run AFTER create_tables.sql. Idempotent: if_not_exists guards every
-- conversion; set_chunk_time_interval only affects future chunks.
--
-- Chunk intervals match the live DB:
--   trip_events  -> 30 days  (spec PDF says "1 month"; 30d is the real value)
--   all others   -> 1 day
-- =====================================================================

SELECT create_hypertable('trip_events',         by_range('pickup_datetime',  INTERVAL '30 days'), if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('hourly_demand',       by_range('time_bucket',      INTERVAL '1 day'),   if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('weather_data',        by_range('observation_time', INTERVAL '1 day'),   if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('hotspot_alerts',      by_range('detected_at',      INTERVAL '1 day'),   if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('forecast_results',    by_range('forecast_time',    INTERVAL '1 day'),   if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('network_flows',       by_range('time_window',      INTERVAL '1 day'),   if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('network_centrality',  by_range('analysis_time',    INTERVAL '1 day'),   if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('spatial_statistics',  by_range('analysis_time',    INTERVAL '1 day'),   if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('temporal_anomalies',  by_range('detected_at',      INTERVAL '1 day'),   if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('trend_decomposition', by_range('analysis_time',    INTERVAL '1 day'),   if_not_exists => TRUE, migrate_data => TRUE);

-- Re-assert chunk intervals (safe to re-run; affects future chunks only).
SELECT set_chunk_time_interval('trip_events',         INTERVAL '30 days');
SELECT set_chunk_time_interval('hourly_demand',       INTERVAL '1 day');
SELECT set_chunk_time_interval('weather_data',        INTERVAL '1 day');
SELECT set_chunk_time_interval('hotspot_alerts',      INTERVAL '1 day');
SELECT set_chunk_time_interval('forecast_results',    INTERVAL '1 day');
SELECT set_chunk_time_interval('network_flows',       INTERVAL '1 day');
SELECT set_chunk_time_interval('network_centrality',  INTERVAL '1 day');
SELECT set_chunk_time_interval('spatial_statistics',  INTERVAL '1 day');
SELECT set_chunk_time_interval('temporal_anomalies',  INTERVAL '1 day');
SELECT set_chunk_time_interval('trend_decomposition', INTERVAL '1 day');