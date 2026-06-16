-- Pre-aggregated summary tables for dashboard temporal panels.
-- These materialize expensive trip_events rollups (19.7M rows) into small
-- lookup tables so Grafana panels read 8-168 rows instead of full-scanning.
-- Re-run after any trip_events reload.

-- Hour x Day-of-week matrix (168 rows). day_of_week: 0=Mon .. 6=Sun (Python weekday()).
DROP TABLE IF EXISTS agg_hour_dow;
CREATE TABLE agg_hour_dow AS
SELECT hour_of_day, day_of_week, COUNT(*)::bigint AS trips
FROM trip_events
GROUP BY hour_of_day, day_of_week;

-- Weekday vs weekend hourly profile (24 rows), averaged per distinct date.
DROP TABLE IF EXISTS agg_hourly_profile;
CREATE TABLE agg_hourly_profile AS
SELECT hour_of_day,
  ROUND((SUM(CASE WHEN NOT is_weekend THEN 1 ELSE 0 END)::float
    / NULLIF(COUNT(DISTINCT CASE WHEN NOT is_weekend THEN pickup_datetime::date END),0))::numeric,0) AS weekday,
  ROUND((SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END)::float
    / NULLIF(COUNT(DISTINCT CASE WHEN is_weekend THEN pickup_datetime::date END),0))::numeric,0) AS weekend
FROM trip_events
GROUP BY hour_of_day;

-- Trip duration distribution (8 ordered buckets).
DROP TABLE IF EXISTS agg_duration;
CREATE TABLE agg_duration AS
SELECT bucket, ord, trips FROM (
  SELECT CASE
      WHEN trip_time_min < 5  THEN '0-5'
      WHEN trip_time_min < 10 THEN '05-10'
      WHEN trip_time_min < 15 THEN '10-15'
      WHEN trip_time_min < 20 THEN '15-20'
      WHEN trip_time_min < 30 THEN '20-30'
      WHEN trip_time_min < 45 THEN '30-45'
      WHEN trip_time_min < 60 THEN '45-60'
      ELSE '60+' END AS bucket,
    MIN(CASE
      WHEN trip_time_min < 5  THEN 0 WHEN trip_time_min < 10 THEN 1
      WHEN trip_time_min < 15 THEN 2 WHEN trip_time_min < 20 THEN 3
      WHEN trip_time_min < 30 THEN 4 WHEN trip_time_min < 45 THEN 5
      WHEN trip_time_min < 60 THEN 6 ELSE 7 END) AS ord,
    COUNT(*)::bigint AS trips
  FROM trip_events
  WHERE trip_time_min >= 0 AND trip_time_min < 240
  GROUP BY bucket
) q;