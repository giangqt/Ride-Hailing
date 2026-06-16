#!/usr/bin/env python3

import argparse
import io
import json
import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd
import psycopg2
import pyarrow.parquet as pq

PROJECT_ROOT = os.path.expanduser("~/ride-hailing")
SRC_COLS = ["pickup_datetime", "dropoff_datetime", "PULocationID",
            "DOLocationID", "trip_miles", "trip_time"]
COPY_COLS = ["pickup_datetime", "dropoff_datetime", "pu_zone_id", "do_zone_id",
             "trip_miles", "trip_time_min", "hour_of_day", "day_of_week",
             "is_weekend", "is_rush_hour"]
PK_COLS = ["pickup_datetime", "pu_zone_id", "do_zone_id", "dropoff_datetime"]


def connect():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "rides"),
        user=os.environ.get("PGUSER", "rides"),
        password=os.environ.get("PGPASSWORD", "rides"),
    )


def week_aligned_offset(source_end: date, anchor_end: date) -> timedelta:
    raw_days = (anchor_end - source_end).days
    return timedelta(days=(raw_days // 7) * 7)  # floor: end never exceeds anchor_end


def load_valid_zones(conn) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT zone_id FROM taxi_zones")
        return {r[0] for r in cur.fetchall()}


def transform(df: pd.DataFrame, offset: timedelta, valid_zones: set) -> pd.DataFrame:
    df = df.copy()
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"]) + offset
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"]) + offset

    df["pu_zone_id"] = df["PULocationID"].where(
        df["PULocationID"].isin(valid_zones), 264).astype(int)
    df["do_zone_id"] = df["DOLocationID"].where(
        df["DOLocationID"].isin(valid_zones), 264).astype(int)

    df["trip_time_min"] = df["trip_time"].astype(float) / 60.0
    df["trip_miles"] = df["trip_miles"].astype(float)

    pu = df["pickup_datetime"]
    df["hour_of_day"] = pu.dt.hour.astype(int)
    df["day_of_week"] = pu.dt.dayofweek.astype(int)  # Monday=0
    df["is_weekend"] = df["day_of_week"].isin([5, 6])
    rush = ((df["hour_of_day"].between(7, 9)) | (df["hour_of_day"].between(16, 19)))
    df["is_rush_hour"] = rush & ~df["is_weekend"]

    return df[COPY_COLS]


def setup_staging(conn):
    with conn.cursor() as cur:
        cur.execute("""CREATE TEMP TABLE IF NOT EXISTS _stage
                       (LIKE trip_events INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS""")
    conn.commit()


def copy_batch(conn, df: pd.DataFrame):
    df = df.drop_duplicates(subset=PK_COLS)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ",".join(COPY_COLS)
    conflict = ",".join(PK_COLS)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE _stage")
        cur.copy_expert(f"COPY _stage ({cols}) FROM STDIN WITH CSV", buf)
        cur.execute(f"INSERT INTO trip_events ({cols}) SELECT {cols} FROM _stage "
                    f"ON CONFLICT ({conflict}) DO NOTHING")
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-file",
                    default=os.path.join(PROJECT_ROOT, "data/raw_trips/fhvhv_tripdata_2024-04.parquet"))
    ap.add_argument("--source-start", default="2024-04-01",
                    help="first source date to load (inclusive)")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--anchor-end", default=date.today().isoformat(),
                    help="map the last source day to this date (week-snapped)")
    ap.add_argument("--batch-rows", type=int, default=500_000)
    ap.add_argument("--checkpoint",
                    default=os.path.join(PROJECT_ROOT, "data/.batch_load_ckpt.json"))
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--fresh", action="store_true",
                      help="TRUNCATE trip_events before loading (default)")
    mode.add_argument("--resume", action="store_true",
                      help="continue from checkpoint without truncating")
    args = ap.parse_args()

    src_start = datetime.fromisoformat(args.source_start)
    src_end = src_start + timedelta(days=args.days)
    offset = week_aligned_offset(src_end.date(), date.fromisoformat(args.anchor_end))
    print(f"[anchor] source {src_start.date()}..{src_end.date()} "
          f"-> shifted by {offset.days} days "
          f"({(src_start + offset).date()}..{(src_end + offset).date()})")

    start_batch = 0
    if args.resume and os.path.exists(args.checkpoint):
        ck = json.load(open(args.checkpoint))
        if ck.get("source_file") == args.source_file:
            start_batch = ck.get("batches_done", 0)
            print(f"[resume] skipping {start_batch} completed batches")

    conn = connect()
    valid_zones = load_valid_zones(conn)
    setup_staging(conn)

    if not args.resume:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE trip_events")
        conn.commit()
        print("[fresh] trip_events truncated")

    pf = pq.ParquetFile(args.source_file)
    total = 0
    for i, rb in enumerate(pf.iter_batches(batch_size=args.batch_rows, columns=SRC_COLS)):
        if i < start_batch:
            continue
        df = rb.to_pandas()
        df = df[(df["pickup_datetime"] >= src_start) & (df["pickup_datetime"] < src_end)]
        if not df.empty:
            copy_batch(conn, transform(df, offset, valid_zones))
            total += len(df)
        json.dump({"source_file": args.source_file, "batches_done": i + 1,
                   "rows_loaded": total}, open(args.checkpoint, "w"))
        print(f"[batch {i}] loaded={len(df):>7}  cumulative={total:,}")

    with conn.cursor() as cur:
        cur.execute("""SELECT COUNT(*), MIN(pickup_datetime), MAX(pickup_datetime),
                       ROUND(EXTRACT(EPOCH FROM MAX(pickup_datetime)-MIN(pickup_datetime))/86400, 2)
                       FROM trip_events""")
        n, mn, mx, span = cur.fetchone()
    print(f"\n[done] trip_events rows={n:,}  window={mn}..{mx}  span_days={span}")
    conn.close()


if __name__ == "__main__":
    sys.exit(main())
