#!/usr/bin/env python3
"""Historical weather backfill: Open-Meteo archive -> weather_data + hourly_demand.

Fetches real April-2024 Central Park hourly observations and shifts them by the
same +770-day anchor used by the trip loader, so weather hours align with the
anchored trip hours. Replaces the synthetic weather from Phase 4.
"""

import argparse
import os
from datetime import datetime, timedelta

import psycopg2
import requests
from psycopg2.extras import execute_values

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
WMO = {0: "Clear", 1: "Clouds", 2: "Clouds", 3: "Clouds", 45: "Fog", 48: "Fog",
       51: "Drizzle", 53: "Drizzle", 55: "Drizzle", 61: "Rain", 63: "Rain",
       65: "Rain", 66: "Rain", 67: "Rain", 71: "Snow", 73: "Snow", 75: "Snow",
       77: "Snow", 80: "Rain", 81: "Rain", 82: "Rain", 85: "Snow", 86: "Snow",
       95: "Thunderstorm", 96: "Thunderstorm", 99: "Thunderstorm"}


def connect():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "rides"),
        user=os.environ.get("PGUSER", "rides"),
        password=os.environ.get("PGPASSWORD", "rides"),
    )


def fetch(lat, lon, start, end):
    r = requests.get(ARCHIVE_URL, params={
        "latitude": lat, "longitude": lon,
        "start_date": start, "end_date": end,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,"
                  "relative_humidity_2m,weather_code",
        "wind_speed_unit": "ms", "timezone": "America/New_York",
    }, timeout=60)
    r.raise_for_status()
    return r.json()["hourly"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lat", type=float, default=40.7794)   # Central Park / KNYC
    ap.add_argument("--lon", type=float, default=-73.9692)
    ap.add_argument("--start", default="2024-04-01")
    ap.add_argument("--end", default="2024-04-30")
    ap.add_argument("--offset-days", type=int, default=770,
                    help="must match the trip loader's anchor offset")
    args = ap.parse_args()

    h = fetch(args.lat, args.lon, args.start, args.end)
    offset = timedelta(days=args.offset_days)
    rows = []
    for i, t in enumerate(h["time"]):
        obs = (datetime.fromisoformat(t) + offset).strftime("%Y-%m-%d %H:%M:%S")
        rows.append((
            obs, h["temperature_2m"][i], h["precipitation"][i],
            h["wind_speed_10m"][i], h["relative_humidity_2m"][i],
            WMO.get(h["weather_code"][i], "Unknown"),
        ))
    print(f"[fetch] {len(rows)} hourly obs "
          f"({rows[0][0]} .. {rows[-1][0]} after +{args.offset_days}d shift)")

    conn = connect()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE weather_data")
        execute_values(cur,
            """INSERT INTO weather_data (observation_time, temperature_c,
               precipitation_mm, wind_speed_ms, humidity_pct, weather_condition)
               VALUES %s""", rows)
        cur.execute("""
            UPDATE hourly_demand h
            SET avg_temperature = w.temperature_c,
                precipitation_mm = w.precipitation_mm
            FROM weather_data w
            WHERE date_trunc('hour', w.observation_time) = h.time_bucket""")
        updated = cur.rowcount
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("""SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE avg_temperature IS NOT NULL),
                       ROUND(AVG(avg_temperature)::numeric, 1),
                       ROUND(SUM(precipitation_mm)::numeric, 1)
                       FROM hourly_demand""")
        total, with_w, mean_t, sum_p = cur.fetchone()
    print(f"[weather_data] {len(rows)} rows inserted")
    print(f"[hourly_demand] {updated} rows updated; "
          f"{with_w}/{total} now have weather; "
          f"mean_temp={mean_t}C  total_precip={sum_p}mm")
    conn.close()


if __name__ == "__main__":
    main()
