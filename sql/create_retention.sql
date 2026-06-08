-- =====================================================================
-- create_retention.sql  ·  Phase 5 Block 6  ·  data retention
-- Run AFTER create_hypertables.sql. Idempotent (policy guarded against
-- timescaledb_information.jobs).
--
-- Per spec: drop trip_events chunks older than 12 months; all analysis
-- tables kept indefinitely (NO retention policy on them).
--
-- NOTE: this policy will NOT delete anything during the thesis lifetime.
-- Replay data is anchored to recent dates (oldest ~28 days old), so no
-- chunk approaches the 12-month threshold. The policy is configured and
-- defensible as lifecycle-management design, but is intentionally inert
-- on current data -- a demo must never delete its own dataset.
-- =====================================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.jobs
        WHERE application_name LIKE 'Retention Policy%'
          AND hypertable_name = 'trip_events'
    ) THEN
        PERFORM add_retention_policy('trip_events', INTERVAL '12 months');
    END IF;
END $$;