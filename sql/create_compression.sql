-- =====================================================================
-- create_compression.sql  ·  Phase 5 Block 5  ·  columnar compression
-- Run AFTER create_hypertables.sql. Idempotent (compress flag is a no-op
-- if already set; policies guarded against timescaledb_information.jobs).
--
-- Threshold: compress chunks older than 7 days (per spec). The replay data
-- spans 2026-05-11..05-27 but sits 2-4 weeks behind the real wall clock, so
-- chunks ARE past the 7-day threshold and compression fires immediately:
-- ~42 of ~45 data chunks compress on first policy run (only the active
-- recent trip_events chunk stays uncompressed). Real storage savings apply.
--
-- segmentby = zone dimension so per-zone Grafana queries stay fast on
-- compressed chunks. orderby = time DESC (default-friendly for recent-first).
--
-- CAVEAT (documented, not blocking): pg_sink upserts trip_events with
-- ON CONFLICT DO UPDATE. TimescaleDB restricts updates on compressed
-- chunks. With --anchor now all timestamps are recent, so upserts target
-- uncompressed (recent) chunks only; compression touches aged chunks the
-- sink no longer writes to. This is safe for the thesis data pattern.
-- =====================================================================

-- ---- enable compression + segmentby per table -----------------------
ALTER TABLE trip_events         SET (timescaledb.compress, timescaledb.compress_segmentby = 'pu_zone_id',     timescaledb.compress_orderby = 'pickup_datetime DESC');
ALTER TABLE hourly_demand       SET (timescaledb.compress, timescaledb.compress_segmentby = 'zone_id',        timescaledb.compress_orderby = 'time_bucket DESC');
ALTER TABLE weather_data        SET (timescaledb.compress, timescaledb.compress_segmentby = 'station_id',     timescaledb.compress_orderby = 'observation_time DESC');
ALTER TABLE hotspot_alerts      SET (timescaledb.compress, timescaledb.compress_segmentby = 'zone_id',        timescaledb.compress_orderby = 'detected_at DESC');
ALTER TABLE forecast_results    SET (timescaledb.compress, timescaledb.compress_segmentby = 'zone_id',        timescaledb.compress_orderby = 'forecast_time DESC');
ALTER TABLE network_flows       SET (timescaledb.compress, timescaledb.compress_segmentby = 'origin_zone_id', timescaledb.compress_orderby = 'time_window DESC');
ALTER TABLE network_centrality  SET (timescaledb.compress, timescaledb.compress_segmentby = 'zone_id',        timescaledb.compress_orderby = 'analysis_time DESC');
ALTER TABLE spatial_statistics  SET (timescaledb.compress, timescaledb.compress_segmentby = 'zone_id',        timescaledb.compress_orderby = 'analysis_time DESC');
ALTER TABLE temporal_anomalies  SET (timescaledb.compress, timescaledb.compress_segmentby = 'zone_id',        timescaledb.compress_orderby = 'detected_at DESC');
ALTER TABLE trend_decomposition SET (timescaledb.compress, timescaledb.compress_segmentby = 'zone_id',        timescaledb.compress_orderby = 'analysis_time DESC');

-- ---- compression policies (guarded; 7-day threshold) -----------------
DO $$
DECLARE
    t TEXT;
    tables TEXT[] := ARRAY[
        'trip_events','hourly_demand','weather_data','hotspot_alerts',
        'forecast_results','network_flows','network_centrality',
        'spatial_statistics','temporal_anomalies','trend_decomposition'];
BEGIN
    FOREACH t IN ARRAY tables LOOP
        -- 2.26 names this 'Columnstore Policy'; older builds 'Compression Policy'
        IF NOT EXISTS (
            SELECT 1 FROM timescaledb_information.jobs
            WHERE (application_name LIKE 'Columnstore Policy%'
                OR application_name LIKE 'Compression Policy%')
              AND hypertable_name = t
        ) THEN
            PERFORM add_compression_policy(t, INTERVAL '7 days');
        END IF;
    END LOOP;
END $$;