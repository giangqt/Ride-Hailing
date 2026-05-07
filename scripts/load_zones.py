"""
scripts/load_zones.py

Populate the taxi_zones table from the two local CSVs under data/zones/:
  - taxi_zone_lookup.csv     (zone_id, borough, zone_name)
  - taxi_zone_centroids.csv  (zone_id, centroid_lat, centroid_lon)

Idempotent: creates the table if missing, upserts on zone_id.
Re-run any time it's safe.

Usage:
    python scripts/load_zones.py

Env vars (with defaults matching docker-compose):
    PG_HOST     localhost
    PG_PORT     5432
    PG_DB       rides
    PG_USER     rides
    PG_PASSWORD rides
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

# --- config ---------------------------------------------------------------
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB", "rides")
PG_USER     = os.getenv("PG_USER", "rides")
PG_PASSWORD = os.getenv("PG_PASSWORD", "rides")

REPO_ROOT     = Path(__file__).resolve().parent.parent
LOOKUP_CSV    = REPO_ROOT / "data" / "zones" / "taxi_zone_lookup.csv"
CENTROIDS_CSV = REPO_ROOT / "data" / "zones" / "taxi_zone_centroids.csv"

DDL = """
CREATE TABLE IF NOT EXISTS taxi_zones (
    zone_id       INTEGER PRIMARY KEY,
    zone_name     VARCHAR(100),
    borough       VARCHAR(50),
    centroid_lat  DOUBLE PRECISION,
    centroid_lon  DOUBLE PRECISION
);
"""

UPSERT = """
INSERT INTO taxi_zones (zone_id, zone_name, borough, centroid_lat, centroid_lon)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (zone_id) DO UPDATE SET
    zone_name    = EXCLUDED.zone_name,
    borough      = EXCLUDED.borough,
    centroid_lat = EXCLUDED.centroid_lat,
    centroid_lon = EXCLUDED.centroid_lon;
"""


def find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    """Return the actual column name matching any candidate (case-insensitive)."""
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    raise KeyError(
        f"None of {candidates} found. Got columns: {list(df.columns)}"
    )


def main() -> int:
    for f in (LOOKUP_CSV, CENTROIDS_CSV):
        if not f.exists():
            sys.exit(f"Missing file: {f}")

    print(f"Reading {LOOKUP_CSV.name} ...")
    lookup = pd.read_csv(LOOKUP_CSV)
    print(f"Reading {CENTROIDS_CSV.name} ...")
    centroids = pd.read_csv(CENTROIDS_CSV)

    # Flexible column resolution — tolerates LocationID / zone_id / id naming
    lid_l   = find_col(lookup,    ["LocationID", "zone_id", "location_id"])
    borough = find_col(lookup,    ["Borough", "borough"])
    zone    = find_col(lookup,    ["Zone", "zone_name", "zone"])

    lid_c = find_col(centroids, ["LocationID", "zone_id", "location_id"])
    lat   = find_col(centroids, ["centroid_lat", "lat", "latitude"])
    lon   = find_col(centroids, ["centroid_lon", "lon", "longitude",
                                 "centroid_lng", "lng"])

    merged = lookup.merge(
        centroids, left_on=lid_l, right_on=lid_c, how="inner"
    )

    rows = [
        (
            int(r[lid_l]),
            str(r[zone])    if pd.notna(r[zone])    else None,
            str(r[borough]) if pd.notna(r[borough]) else None,
            float(r[lat])   if pd.notna(r[lat])     else None,
            float(r[lon])   if pd.notna(r[lon])     else None,
        )
        for _, r in merged.iterrows()
    ]
    print(f"Prepared {len(rows)} rows from CSVs.")

    if len(rows) < 260:
        print(f"WARNING: expected ~263 zones, got {len(rows)}. "
              f"Check that both CSVs have matching zone IDs.")

    print(f"Connecting to {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB} ...")
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            execute_batch(cur, UPSERT, rows, page_size=500)
            cur.execute("SELECT COUNT(*) FROM taxi_zones;")
            (n,) = cur.fetchone()

    print(f"Done. taxi_zones now has {n} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())