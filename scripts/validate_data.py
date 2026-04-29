#!/usr/bin/env python3
"""
validate_data.py
================

Validates a downloaded NYC TLC FHVHV Parquet file against the rules defined
in scripts/config.py. Produces a machine-readable JSON report under
data/validation_reports/ and exits with a non-zero status code on failure
so cron / Airflow / a CI runner can alert.

Checks performed:
    1. File readable as Parquet
    2. Row count >= VALIDATION_MIN_ROWS
    3. All VALIDATION_REQUIRED_COLS present (after FHVHV column normalization)
    4. Null fraction on VALIDATION_CRITICAL_COLS below threshold
    5. PULocationID / DOLocationID within [VALIDATION_ZONE_ID_MIN, MAX]
    6. pickup_datetime range falls within the file's expected month
       (inferred from filename)

Usage:
    python scripts/validate_data.py                  # validates ALL files
    python scripts/validate_data.py --latest         # latest file only
    python scripts/validate_data.py --file PATH      # specific file

Exit codes:
    0 - all checks passed
    1 - one or more files failed validation
    2 - bad CLI arguments / file not found

Pipeline stage: Stage 1 (Data Collection) - runs after download_tlc.py.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import (
    RAW_TRIPS_DIR,
    VALIDATION_CRITICAL_COLS,
    VALIDATION_DIR,
    VALIDATION_MAX_NULL_FRAC,
    VALIDATION_MIN_ROWS,
    VALIDATION_REQUIRED_COLS,
    VALIDATION_ZONE_ID_MAX,
    VALIDATION_ZONE_ID_MIN,
)
from logger import get_logger

log = get_logger("validate_data")

_FNAME_RE = re.compile(r"fhvhv_tripdata_(\d{4})-(\d{2})\.parquet$")

# FHVHV files use 'pickup_datetime' / 'dropoff_datetime' but older TLC schemas
# used 'pickup_ts'. We accept both and normalize.
_PICKUP_ALIASES = ("pickup_datetime", "pickup_ts")
_DROPOFF_ALIASES = ("dropoff_datetime", "dropoff_ts")


def _resolve_col(present_cols: set[str], aliases: tuple[str, ...]) -> str | None:
    for c in aliases:
        if c in present_cols:
            return c
    return None


def _expected_month_from_name(path: Path) -> tuple[int, int] | None:
    m = _FNAME_RE.search(path.name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _validate_one(path: Path) -> dict[str, Any]:
    """Run all checks against one parquet file. Returns a report dict."""
    report: dict[str, Any] = {
        "file": str(path),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "passed": False,
        "checks": {},
        "errors": [],
    }

    # --- 1. Readable parquet --------------------------------------------
    try:
        import pyarrow.parquet as pq  # type: ignore
        pf = pq.ParquetFile(path)
        n_rows = pf.metadata.num_rows
        schema = pf.schema_arrow
        present_cols = {f.name for f in schema}
    except Exception as e:
        report["errors"].append(f"unreadable parquet: {e}")
        return report

    report["checks"]["row_count"] = {"value": n_rows, "min": VALIDATION_MIN_ROWS,
                                     "ok": n_rows >= VALIDATION_MIN_ROWS}
    if n_rows < VALIDATION_MIN_ROWS:
        report["errors"].append(
            f"row_count {n_rows:,} < threshold {VALIDATION_MIN_ROWS:,}")

    # --- 2. Required columns -------------------------------------------
    pickup_col = _resolve_col(present_cols, _PICKUP_ALIASES)
    dropoff_col = _resolve_col(present_cols, _DROPOFF_ALIASES)

    required_present = []
    for col in VALIDATION_REQUIRED_COLS:
        if col == "pickup_datetime":
            required_present.append(pickup_col is not None)
        elif col == "dropoff_datetime":
            required_present.append(dropoff_col is not None)
        else:
            required_present.append(col in present_cols)

    missing = [c for c, ok in zip(VALIDATION_REQUIRED_COLS, required_present) if not ok]
    report["checks"]["required_columns"] = {
        "missing": missing, "ok": len(missing) == 0,
    }
    if missing:
        report["errors"].append(f"missing required columns: {missing}")

    # --- 3-6 require pandas + a single read of the columns we care about
    cols_to_read = [c for c in
                    {"PULocationID", "DOLocationID", pickup_col, dropoff_col}
                    if c is not None and c in present_cols]
    if not cols_to_read:
        # We've already logged missing required columns; nothing more we can do.
        return report

    try:
        import pandas as pd  # noqa: F401
        # read_columns is faster than to_pandas() with all columns.
        table = pf.read(columns=cols_to_read)
        df = table.to_pandas()
    except Exception as e:
        report["errors"].append(f"failed to load columns into pandas: {e}")
        return report

    # --- 3. Null fractions on critical cols ----------------------------
    null_report: dict[str, dict[str, Any]] = {}
    for col_logical in VALIDATION_CRITICAL_COLS:
        actual = (pickup_col if col_logical == "pickup_datetime"
                  else dropoff_col if col_logical == "dropoff_datetime"
                  else col_logical)
        if actual is None or actual not in df.columns:
            null_report[col_logical] = {"ok": False, "reason": "column missing"}
            report["errors"].append(f"critical column {col_logical} missing")
            continue
        frac = float(df[actual].isna().mean())
        ok = frac <= VALIDATION_MAX_NULL_FRAC
        null_report[col_logical] = {
            "null_fraction": round(frac, 6),
            "max_allowed": VALIDATION_MAX_NULL_FRAC,
            "ok": ok,
        }
        if not ok:
            report["errors"].append(
                f"{col_logical} null fraction {frac:.4f} > {VALIDATION_MAX_NULL_FRAC}")
    report["checks"]["null_fractions"] = null_report

    # --- 4. Zone-id ranges ---------------------------------------------
    zone_check: dict[str, dict[str, Any]] = {}
    for col in ("PULocationID", "DOLocationID"):
        if col not in df.columns:
            continue
        s = df[col].dropna()
        if s.empty:
            zone_check[col] = {"ok": False, "reason": "all null"}
            report["errors"].append(f"{col} all null")
            continue
        mn, mx = int(s.min()), int(s.max())
        ok = mn >= VALIDATION_ZONE_ID_MIN and mx <= VALIDATION_ZONE_ID_MAX
        zone_check[col] = {"min": mn, "max": mx, "ok": ok}
        if not ok:
            report["errors"].append(
                f"{col} range [{mn},{mx}] outside [{VALIDATION_ZONE_ID_MIN},{VALIDATION_ZONE_ID_MAX}]")
    report["checks"]["zone_id_range"] = zone_check

    # --- 5. Datetime range matches expected month -----------------------
    expected = _expected_month_from_name(path)
    if pickup_col and pickup_col in df.columns and expected:
        ts = df[pickup_col].dropna()
        if not ts.empty:
            min_ts, max_ts = ts.min(), ts.max()
            ok = (min_ts.year == expected[0] and max_ts.year == expected[0]
                  and min_ts.month == expected[1] and max_ts.month == expected[1])
            report["checks"]["datetime_range"] = {
                "expected_year": expected[0],
                "expected_month": expected[1],
                "min": str(min_ts),
                "max": str(max_ts),
                "ok": ok,
            }
            if not ok:
                report["errors"].append(
                    f"pickup_datetime range [{min_ts}, {max_ts}] does not match "
                    f"expected {expected[0]}-{expected[1]:02d}")

    report["passed"] = len(report["errors"]) == 0
    return report


def _write_report(report: dict[str, Any]) -> Path:
    src = Path(report["file"]).stem
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = VALIDATION_DIR / f"{src}__{stamp}.json"
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return out


def _summarize(report: dict[str, Any]) -> str:
    if report["passed"]:
        rc = report["checks"].get("row_count", {}).get("value", "?")
        return f"PASS  {Path(report['file']).name}  rows={rc}"
    return (f"FAIL  {Path(report['file']).name}  "
            f"errors={len(report['errors'])}: {report['errors'][:2]}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--file", type=Path, help="Path to a single parquet file")
    grp.add_argument("--latest", action="store_true",
                     help="Validate only the most recently modified file")
    args = parser.parse_args()

    if args.file:
        files = [args.file]
    else:
        files = sorted(RAW_TRIPS_DIR.glob("fhvhv_tripdata_*.parquet"))
        if args.latest and files:
            files = [max(files, key=lambda p: p.stat().st_mtime)]

    if not files:
        log.error("No files to validate (looked in %s).", RAW_TRIPS_DIR)
        return 2

    log.info("Validating %d file(s).", len(files))
    any_fail = False
    for p in files:
        if not p.exists():
            log.error("File not found: %s", p)
            any_fail = True
            continue

        log.info("Validating %s ...", p.name)
        report = _validate_one(p)
        out = _write_report(report)
        log.info("  -> %s | report=%s", _summarize(report), out.name)
        if not report["passed"]:
            any_fail = True

    log.info("Validation finished. status=%s", "FAIL" if any_fail else "PASS")
    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
