#!/usr/bin/env python3
"""
download_tlc.py
===============

Monthly NYC TLC High Volume FHV (Uber/Lyft) Parquet downloader.

Behavior:
    * Default mode: detect "what's missing" since TLC_FHVHV_FIRST_MONTH up
      to today minus TLC_PUBLICATION_LAG_MONTHS, and download anything not
      already on disk.
    * --month YYYY-MM:  download exactly that month.
    * --since YYYY-MM:  download every month from `since` to the latest
      available month.
    * --force:          re-download even if the file is on disk.

The script is idempotent: if a file exists and validates as a readable
Parquet with at least one row group, it is left alone (unless --force).
After each download we run a quick parquet-readable check; if that fails
the partial file is deleted and the script exits non-zero so cron can
alert.

Cron schedule (per spec):     0 2 1 * *   (monthly, 02:00 on day 1)
Output:                       data/raw_trips/fhvhv_tripdata_YYYY-MM.parquet
Pipeline stage:               Stage 1 (Data Collection)
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Iterable

import requests

from config import (
    RAW_TRIPS_DIR,
    TLC_BASE_URL,
    TLC_FHVHV_FIRST_MONTH,
    TLC_FILENAME_TEMPLATE,
    TLC_PUBLICATION_LAG_MONTHS,
)
from logger import get_logger

log = get_logger("download_tlc")


# ---------------------------------------------------------------------------
# Month iteration helpers
# ---------------------------------------------------------------------------
def _parse_month(s: str) -> tuple[int, int]:
    """Parse 'YYYY-MM' into (year, month). Raises ValueError on bad input."""
    try:
        y, m = s.strip().split("-")
        year, month = int(y), int(m)
    except Exception as exc:
        raise ValueError(f"bad --month value {s!r}, expected YYYY-MM") from exc
    if not (2000 <= year <= 2100 and 1 <= month <= 12):
        raise ValueError(f"month {s} out of plausible range")
    return year, month


def _month_iter(start: tuple[int, int], end: tuple[int, int]) -> Iterable[tuple[int, int]]:
    """Yield (year, month) tuples inclusive from start to end."""
    y, m = start
    ey, em = end
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


def _latest_available_month(today: date) -> tuple[int, int]:
    """The latest month TLC is likely to have published, given lag."""
    y, m = today.year, today.month - TLC_PUBLICATION_LAG_MONTHS
    while m <= 0:
        m += 12
        y -= 1
    return y, m


# ---------------------------------------------------------------------------
# Download + validation
# ---------------------------------------------------------------------------
def _file_for(year: int, month: int) -> Path:
    return RAW_TRIPS_DIR / TLC_FILENAME_TEMPLATE.format(year=year, month=month)


def _url_for(year: int, month: int) -> str:
    return f"{TLC_BASE_URL}/{TLC_FILENAME_TEMPLATE.format(year=year, month=month)}"


def _quick_parquet_ok(path: Path) -> bool:
    """Open the parquet and read its metadata. Cheap structural check only."""
    try:
        import pyarrow.parquet as pq  # type: ignore
        pf = pq.ParquetFile(path)
        return pf.metadata is not None and pf.metadata.num_rows > 0
    except Exception as e:
        log.warning("Parquet check failed for %s: %s", path.name, e)
        return False


def _download_one(year: int, month: int, force: bool = False) -> bool:
    """Download a single month. Returns True on success / already-present."""
    dest = _file_for(year, month)
    url = _url_for(year, month)

    if dest.exists() and not force:
        if _quick_parquet_ok(dest):
            log.info("Skip %s (already present, %.1f MB)",
                     dest.name, dest.stat().st_size / 1024 / 1024)
            return True
        log.warning("File %s exists but is unreadable; re-downloading", dest.name)
        dest.unlink()

    log.info("GET %s", url)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with requests.get(url, stream=True, timeout=120) as r:
            if r.status_code == 404:
                log.error("Not found (404): %s - has TLC published this month yet?", url)
                return False
            r.raise_for_status()

            total = int(r.headers.get("Content-Length", 0))
            written = 0
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
                        written += len(chunk)
            log.info("Downloaded %s: %.1f MB%s",
                     dest.name,
                     written / 1024 / 1024,
                     f" / expected {total / 1024 / 1024:.1f} MB" if total else "")

        tmp.replace(dest)

        if not _quick_parquet_ok(dest):
            log.error("Downloaded file %s failed parquet structural check", dest.name)
            dest.unlink()
            return False

        return True

    except requests.RequestException as e:
        log.error("Download failed for %s: %s", url, e)
        if tmp.exists():
            tmp.unlink()
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--month", type=str, help="Single month YYYY-MM")
    grp.add_argument("--since", type=str,
                     help="Download every month from YYYY-MM up to latest available")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if file already on disk")
    args = parser.parse_args()

    today = date.today()
    latest = _latest_available_month(today)

    # Build the list of months to attempt.
    if args.month:
        months = [_parse_month(args.month)]
    elif args.since:
        months = list(_month_iter(_parse_month(args.since), latest))
    else:
        # Default cron behavior: fill in any gaps from FHVHV start to latest.
        months = list(_month_iter(TLC_FHVHV_FIRST_MONTH, latest))
        # Filter out months we already have & validate (cheap).
        if not args.force:
            months = [
                ym for ym in months
                if not (_file_for(*ym).exists() and _quick_parquet_ok(_file_for(*ym)))
            ]
        if not months:
            log.info("Nothing to do - all months from %d-%02d to %d-%02d already present.",
                     *TLC_FHVHV_FIRST_MONTH, *latest)
            return 0
        log.info("Cron mode: %d missing month(s) to fetch.", len(months))

    log.info("Plan: %d month(s): %s",
             len(months),
             ", ".join(f"{y}-{m:02d}" for y, m in months))

    ok, fail = 0, 0
    for y, m in months:
        if _download_one(y, m, force=args.force):
            ok += 1
        else:
            fail += 1

    log.info("Done. ok=%d fail=%d", ok, fail)
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
