-- =====================================================================
-- create_indexes.sql  ·  Phase 5 Block 3  ·  flag-index gap fill
-- Run AFTER create_hypertables.sql. Idempotent (IF NOT EXISTS).
--
-- The base time-DESC and (zone_id, time DESC) indexes already exist on
-- every table from the Block 6 sink DDL and the Colab notebooks. The two
-- flag indexes below are the only ones the spec calls for that are missing.
-- Built as PARTIAL indexes (WHERE flag) + time DESC: they index only the
-- rows the Grafana panels actually filter on (significant hotspots /
-- detected anomalies, most recent first) rather than the full column.
-- =====================================================================

CREATE INDEX IF NOT EXISTS idx_spatial_stats_significant
    ON spatial_statistics (analysis_time DESC)
    WHERE is_significant_hotspot = TRUE;

CREATE INDEX IF NOT EXISTS idx_temporal_anomalies_flagged
    ON temporal_anomalies (detected_at DESC)
    WHERE is_anomaly = TRUE;