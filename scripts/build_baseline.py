"""
Build per-zone, per-hour-of-week demand baseline from historical TLC parquet files.

Reads:  data/raw_trips/fhvhv_tripdata_*.parquet
Writes: data/baseline/zone_demand_baseline.csv

Output schema (44,184 rows = 263 zones x 168 hours-of-week):
    zone_id              INT     TLC PULocationID (1..263)
    hour_of_week         INT     0..167, computed as day_of_week * 24 + hour_of_day
                                 with Monday=0, in America/New_York timezone
    baseline_mean_pickups FLOAT  Mean hourly pickup count for this (zone, hour-of-week)
                                 across all *complete* weeks in the input data.
                                 Cells with no historical pickups are filled with 0.0.

Convention shared with spark/aggregation.py:
    HOUR_OF_WEEK = (dayofweek_monday_zero * 24) + hour_of_day, in America/New_York.
    Spark's F.dayofweek() returns 1=Sunday..7=Saturday; we shift to 0=Monday..6=Sunday.

Run:
    python scripts/build_baseline.py
"""
from __future__ import annotations

import glob
import logging
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

# Allow `python scripts/build_baseline.py` from project root by adding repo root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from spark._session import get_session  # noqa: E402

# --- Constants -----------------------------------------------------------------

NYC_TZ = "America/New_York"
RAW_TRIPS_GLOB = str(PROJECT_ROOT / "data" / "raw_trips" / "fhvhv_tripdata_*.parquet")
OUTPUT_DIR = PROJECT_ROOT / "data" / "baseline"
OUTPUT_FILE = OUTPUT_DIR / "zone_demand_baseline.csv"
TMP_OUTPUT_DIR = OUTPUT_DIR / "_tmp_baseline_csv"

NUM_ZONES = 263            # TLC PULocationID range: 1..263
HOURS_PER_WEEK = 168       # 7 days * 24 hours
EXPECTED_ROWS = NUM_ZONES * HOURS_PER_WEEK  # 44,184

BELOW_FLOOR_THRESHOLD = 1.0  # zones with baseline < 1 pickup/hour flagged for Block 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("build_baseline")


# --- Pure transforms (importable for unit testing) -----------------------------

def add_hour_of_week(df, ts_col: str = "pickup_datetime"):
    """Convert a UTC/naive timestamp column to America/New_York and derive hour_of_week.

    Spark's dayofweek() returns 1=Sunday..7=Saturday. We want Monday=0..Sunday=6,
    so the mapping is: dow_monday_zero = ((dayofweek + 5) % 7).
    """
    local_ts = F.col(ts_col)
    dow_monday_zero = (F.dayofweek(local_ts) + F.lit(5)) % F.lit(7)
    return df.withColumn(
        "hour_of_week",
        (dow_monday_zero * F.lit(24) + F.hour(local_ts)).cast("int"),
    ).withColumn(
        "_local_ts", local_ts
    )


def determine_complete_weeks(df, ts_col: str = "_local_ts"):
    """Return (df_filtered, num_complete_weeks).

    Drops rows from the first and last partial weeks of the input range so the
    mean is computed over uniform 7-day blocks.
    """
    bounds = df.agg(
        F.min(ts_col).alias("min_ts"),
        F.max(ts_col).alias("max_ts"),
    ).collect()[0]
    min_ts, max_ts = bounds["min_ts"], bounds["max_ts"]
    if min_ts is None or max_ts is None:
        raise ValueError("No data found in input parquet files after null filtering.")

    # Anchor weeks to ISO week boundaries: Monday 00:00 local time
    # Spark's weekofyear is ISO week (Monday-start), which matches our convention.
    df_with_week = df.withColumn(
        "_week_key",
        F.concat_ws("-", F.year(ts_col), F.weekofyear(ts_col)),
    )

    # Count rows per week; complete weeks should have roughly the same row count.
    # Simpler heuristic: drop the first and last week_key seen in the data.
    week_keys = [r["_week_key"] for r in df_with_week.select("_week_key").distinct()
                 .orderBy("_week_key").collect()]
    if len(week_keys) < 3:
        raise ValueError(
            f"Need at least 3 weeks of data to drop partials; got {len(week_keys)}."
        )

    complete_weeks = week_keys[1:-1]
    log.info(
        "Week range: %s ... %s (%d total weeks; dropping first and last partial; "
        "%d complete weeks remain)",
        week_keys[0], week_keys[-1], len(week_keys), len(complete_weeks),
    )
    df_filtered = df_with_week.filter(F.col("_week_key").isin(complete_weeks))
    return df_filtered, len(complete_weeks)


def build_baseline_df(spark, trips_df, num_complete_weeks: int):
    """Aggregate trips into per-(zone, hour-of-week) mean pickups.

    Cross-joins 263 zones x 168 hours-of-week so that cells with zero historical
    pickups appear with baseline_mean_pickups = 0.0 (not missing).
    """
    counts = (
        trips_df.groupBy("zone_id", "hour_of_week")
        .count()
        .withColumn(
            "baseline_mean_pickups",
            (F.col("count") / F.lit(num_complete_weeks)).cast("double"),
        )
        .drop("count")
    )

    zones = spark.range(1, NUM_ZONES + 1).withColumnRenamed("id", "zone_id")
    hours = spark.range(0, HOURS_PER_WEEK).withColumnRenamed("id", "hour_of_week")
    grid = zones.crossJoin(hours)

    baseline = (
        grid.join(counts, ["zone_id", "hour_of_week"], "left")
        .fillna(0.0, subset=["baseline_mean_pickups"])
        .select(
            F.col("zone_id").cast("int").alias("zone_id"),
            F.col("hour_of_week").cast("int").alias("hour_of_week"),
            F.col("baseline_mean_pickups").cast("double").alias("baseline_mean_pickups"),
        )
        .orderBy("zone_id", "hour_of_week")
    )
    return baseline


# --- Validation logging --------------------------------------------------------

def log_validation(baseline_df, taxi_zones_path: Path | None = None):
    """Sanity checks: row count, zone coverage, percentiles, top/bottom zones."""
    total = baseline_df.count()
    distinct_zones = baseline_df.select("zone_id").distinct().count()
    distinct_hours = baseline_df.select("hour_of_week").distinct().count()

    log.info("Output rows: %d (expected %d)", total, EXPECTED_ROWS)
    log.info("Distinct zones: %d (expected %d)", distinct_zones, NUM_ZONES)
    log.info("Distinct hour_of_week: %d (expected %d)", distinct_hours, HOURS_PER_WEEK)

    if total != EXPECTED_ROWS:
        log.warning("Row count mismatch — investigate before using this baseline.")

    # Per-cell distribution
    quantiles = baseline_df.approxQuantile(
        "baseline_mean_pickups", [0.5, 0.9, 0.99], 0.001
    )
    stats = baseline_df.agg(
        F.min("baseline_mean_pickups").alias("min"),
        F.max("baseline_mean_pickups").alias("max"),
        F.mean("baseline_mean_pickups").alias("mean"),
    ).collect()[0]
    log.info(
        "baseline_mean_pickups (per zone-hour cell): "
        "min=%.3f  p50=%.3f  p90=%.3f  p99=%.3f  max=%.3f  mean=%.3f",
        stats["min"], quantiles[0], quantiles[1], quantiles[2],
        stats["max"], stats["mean"],
    )

    # Below-floor cells (Block 4 will need a divide-by-zero guard)
    below = baseline_df.filter(
        F.col("baseline_mean_pickups") < F.lit(BELOW_FLOOR_THRESHOLD)
    ).count()
    pct = (below / total * 100.0) if total else 0.0
    log.info(
        "Cells with baseline < %.1f pickups/hour: %d (%.1f%% of total) — "
        "Block 4 will need a floor or div-by-zero guard for these.",
        BELOW_FLOOR_THRESHOLD, below, pct,
    )

    # Top/bottom zones by mean baseline (averaged across all 168 hours of week)
    zone_means = (
        baseline_df.groupBy("zone_id")
        .agg(F.mean("baseline_mean_pickups").alias("zone_mean"))
        .orderBy(F.col("zone_mean").desc())
    )

    top = zone_means.limit(5).collect()
    bottom = zone_means.orderBy(F.col("zone_mean").asc()).limit(5).collect()

    log.info("Top 5 zones by mean baseline (sanity check — expect Manhattan core):")
    for r in top:
        log.info("    zone_id=%-4d  mean_pickups/hr=%.2f", r["zone_id"], r["zone_mean"])

    log.info("Bottom 5 zones by mean baseline:")
    for r in bottom:
        log.info("    zone_id=%-4d  mean_pickups/hr=%.2f", r["zone_id"], r["zone_mean"])


# --- IO helpers ----------------------------------------------------------------

def write_single_csv(baseline_df, tmp_dir: Path, final_path: Path) -> None:
    """Spark writes a directory of part-files; coalesce to 1 and rename to a flat CSV."""
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    (
        baseline_df.coalesce(1)
        .write.option("header", "true")
        .mode("overwrite")
        .csv(str(tmp_dir))
    )

    # Find the single part-*.csv file Spark produced and move it into place
    part_files = list(tmp_dir.glob("part-*.csv"))
    if len(part_files) != 1:
        raise RuntimeError(
            f"Expected exactly 1 part file in {tmp_dir}, found {len(part_files)}."
        )

    final_path.parent.mkdir(parents=True, exist_ok=True)
    if final_path.exists():
        final_path.unlink()
    shutil.move(str(part_files[0]), str(final_path))
    shutil.rmtree(tmp_dir)
    log.info("Wrote baseline to %s", final_path)


# --- Main ----------------------------------------------------------------------

def main() -> int:
    parquet_files = sorted(glob.glob(RAW_TRIPS_GLOB))
    if not parquet_files:
        log.error("No parquet files matched %s", RAW_TRIPS_GLOB)
        return 1
    log.info("Found %d parquet file(s):", len(parquet_files))
    for p in parquet_files:
        log.info("    %s (%.1f MB)", p, os.path.getsize(p) / 1024 / 1024)

    spark: SparkSession = get_session(app_name="build_baseline")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Read with projection-pushdown — only the two columns we need
        raw = spark.read.parquet(*parquet_files).select(
            F.col("pickup_datetime").alias("pickup_datetime"),
            F.col("PULocationID").alias("zone_id"),
        )
        raw_count = raw.count()
        log.info("Total rows read: %d", raw_count)

        # 2. Drop nulls (mirrors enrichment.py data-quality contract)
        clean = raw.filter(
            F.col("pickup_datetime").isNotNull()
            & F.col("zone_id").isNotNull()
        )
        clean_count = clean.count()
        dropped = raw_count - clean_count
        log.info(
            "After null filter: %d rows (dropped %d, %.3f%%)",
            clean_count, dropped, (dropped / raw_count * 100.0) if raw_count else 0.0,
        )

        # 3. Derive hour_of_week in America/New_York
        with_how = add_hour_of_week(clean, ts_col="pickup_datetime")

        # 4. Restrict to complete weeks
        complete, num_weeks = determine_complete_weeks(with_how, ts_col="_local_ts")
        if num_weeks < 1:
            log.error("No complete weeks remain after filtering partials.")
            return 1

        # 5. Aggregate to (zone, hour-of-week) means with full 263x168 grid
        baseline = build_baseline_df(spark, complete, num_weeks).cache()

        # 6. Validate
        log_validation(baseline)

        # 7. Write single CSV
        write_single_csv(baseline, TMP_OUTPUT_DIR, OUTPUT_FILE)

        log.info("Baseline build complete.")
        return 0

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())