#!/usr/bin/env python3
"""
download_zones.py
=================

One-time setup script: downloads the NYC TLC Taxi Zone shapefile (263 zones)
from CloudFront, unzips it, converts to GeoJSON in WGS84 (EPSG:4326), and
exports a small centroids CSV used by Grafana Geomap and the Spark
enrichment broadcast join.

Outputs (under data/zones/):
    taxi_zones.zip                  raw download
    taxi_zones.shp / .dbf / .prj    unzipped shapefile (NAD83 / projected)
    taxi_zones.geojson              WGS84 GeoJSON (used by Spark enrichment)
    taxi_zone_lookup.csv            zone -> name / borough lookup
    taxi_zone_centroids.csv         zone_id, name, borough, lat, lon

Run:
    python scripts/download_zones.py            # idempotent, skips if present
    python scripts/download_zones.py --force    # re-download even if present

Pipeline stage: Stage 1 (Data Collection) - one-time.
"""
from __future__ import annotations

import argparse
import csv
import sys
import zipfile
from pathlib import Path

import requests

from config import (
    TLC_ZONES_LOOKUP_CSV_URL,
    TLC_ZONES_SHAPEFILE_URL,
    ZONES_DIR,
)
from logger import get_logger

log = get_logger("download_zones")

ZIP_PATH = ZONES_DIR / "taxi_zones.zip"
SHP_PATH = ZONES_DIR / "taxi_zones.shp"
GEOJSON_PATH = ZONES_DIR / "taxi_zones.geojson"
LOOKUP_CSV_PATH = ZONES_DIR / "taxi_zone_lookup.csv"
CENTROIDS_CSV_PATH = ZONES_DIR / "taxi_zone_centroids.csv"


def _stream_download(url: str, dest: Path, chunk_size: int = 1 << 16) -> None:
    """Stream a URL to disk, raising on HTTP errors."""
    log.info("Downloading %s -> %s", url, dest.name)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        written = 0
        tmp = dest.with_suffix(dest.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    written += len(chunk)
        tmp.replace(dest)
    log.info("Wrote %s (%.2f MB%s)",
             dest.name,
             written / 1024 / 1024,
             f", expected {total / 1024 / 1024:.2f} MB" if total else "")


def _unzip(zip_path: Path, dest_dir: Path) -> None:
    log.info("Unzipping %s", zip_path.name)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(dest_dir)
    extracted = sorted(p.name for p in dest_dir.glob("taxi_zones.*"))
    log.info("Extracted: %s", ", ".join(extracted))


def _shapefile_to_geojson(shp_path: Path, geojson_path: Path) -> int:
    """Convert shapefile to WGS84 GeoJSON. Returns row count."""
    # GeoPandas import is local so users running just download_tlc.py don't
    # need geopandas installed.
    import geopandas as gpd  # type: ignore

    log.info("Reading shapefile %s", shp_path.name)
    gdf = gpd.read_file(shp_path)

    if gdf.crs is None:
        log.warning("Shapefile has no CRS metadata; assuming EPSG:2263 (NY)")
        gdf = gdf.set_crs(epsg=2263)

    log.info("Reprojecting %s -> EPSG:4326", gdf.crs)
    gdf_wgs84 = gdf.to_crs(epsg=4326)

    log.info("Writing GeoJSON %s", geojson_path.name)
    if geojson_path.exists():
        geojson_path.unlink()
    gdf_wgs84.to_file(geojson_path, driver="GeoJSON")
    return len(gdf_wgs84)


def _export_centroids(shp_path: Path, csv_path: Path) -> int:
    """Compute zone centroids in WGS84 and dump to CSV.

    Centroids are computed in the **projected** CRS (feet) and only then
    reprojected to WGS84, otherwise centroid coordinates are geometrically
    incorrect on lat/lon.
    """
    import geopandas as gpd  # type: ignore

    gdf = gpd.read_file(shp_path)
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=2263)

    centroids_proj = gdf.geometry.centroid
    centroids_wgs = centroids_proj.to_crs(epsg=4326)

    rows = []
    for idx, row in gdf.iterrows():
        c = centroids_wgs.iloc[idx]
        rows.append({
            "zone_id": int(row["LocationID"]),
            "zone_name": row.get("zone", ""),
            "borough": row.get("borough", ""),
            "centroid_lat": float(c.y),
            "centroid_lon": float(c.x),
        })

    rows.sort(key=lambda r: r["zone_id"])
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    log.info("Wrote %d zone centroids -> %s", len(rows), csv_path.name)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if files already exist.")
    args = parser.parse_args()

    if GEOJSON_PATH.exists() and CENTROIDS_CSV_PATH.exists() and not args.force:
        log.info("Zones already present (%s, %s). Use --force to re-download.",
                 GEOJSON_PATH.name, CENTROIDS_CSV_PATH.name)
        return 0

    try:
        _stream_download(TLC_ZONES_SHAPEFILE_URL, ZIP_PATH)
        _unzip(ZIP_PATH, ZONES_DIR)
        _stream_download(TLC_ZONES_LOOKUP_CSV_URL, LOOKUP_CSV_PATH)

        n_zones = _shapefile_to_geojson(SHP_PATH, GEOJSON_PATH)
        n_centroids = _export_centroids(SHP_PATH, CENTROIDS_CSV_PATH)

        if n_zones != n_centroids:
            log.warning("Mismatch: %d geometries vs %d centroids",
                        n_zones, n_centroids)

        # Spec says 263 zones; accept the file as long as it's in [260, 270]
        # to allow for minor TLC updates.
        if not (260 <= n_zones <= 270):
            log.error("Unexpected zone count %d (expected ~263)", n_zones)
            return 2

        log.info("OK: %d zones written.", n_zones)
        return 0

    except requests.HTTPError as e:
        log.error("HTTP error: %s", e)
        return 1
    except Exception as e:
        log.exception("Unexpected error: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
