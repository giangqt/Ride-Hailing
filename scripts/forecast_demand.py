#!/usr/bin/env python3
"""Demand forecasting: SARIMAX + ETS per top-N zone -> forecast_results.

Replaces the Phase 4 pmdarima path (49 MB pickles from embedded training history)
with statsmodels SARIMAX (KB-scale results). Uses the full 30-day window:
23-day train / 7-day test, hourly series with daily seasonality (s=24).
Picks the lower-MAE model per zone and writes its test-window forecast.
"""

import os
import pickle
import warnings

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.statespace.sarimax import SARIMAX

warnings.filterwarnings("ignore")

TOP_N = 20
TEST_HOURS = 168          # 7-day test/forecast window
SEASONAL = 24
SARIMAX_ORDER = (2, 1, 2)
SARIMAX_SEASONAL = (1, 1, 1, SEASONAL)
MODELS_DIR = os.path.expanduser("~/ride-hailing/models")


def connect():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "rides"),
        user=os.environ.get("PGUSER", "rides"),
        password=os.environ.get("PGPASSWORD", "rides"),
    )


def load_series(conn):
    df = pd.read_sql("""
        SELECT time_bucket, zone_id, pickup_count
        FROM hourly_demand ORDER BY time_bucket""", conn)
    df["time_bucket"] = pd.to_datetime(df["time_bucket"])
    full_idx = pd.date_range(df["time_bucket"].min(), df["time_bucket"].max(), freq="h")
    top = (df.groupby("zone_id")["pickup_count"].sum()
             .sort_values(ascending=False).head(TOP_N).index.tolist())
    series = {}
    for z in top:
        s = (df[df.zone_id == z].set_index("time_bucket")["pickup_count"]
             .reindex(full_idx, fill_value=0).astype(float))
        series[z] = s
    return series


def fit_eval(s):
    train, test = s.iloc[:-TEST_HOURS], s.iloc[-TEST_HOURS:]

    ets = ExponentialSmoothing(train, trend="add", seasonal="add",
                               seasonal_periods=SEASONAL).fit()
    ets_fc = ets.forecast(TEST_HOURS)
    ets_mae = float(np.mean(np.abs(ets_fc.values - test.values)))

    sar = SARIMAX(train, order=SARIMAX_ORDER, seasonal_order=SARIMAX_SEASONAL,
                  enforce_stationarity=False, enforce_invertibility=False).fit(disp=False)
    sar_res = sar.get_forecast(TEST_HOURS)
    sar_fc = sar_res.predicted_mean
    sar_mae = float(np.mean(np.abs(sar_fc.values - test.values)))

    if sar_mae <= ets_mae:
        ci = sar_res.conf_int(alpha=0.05)
        return ("SARIMAX", sar_mae, sar, test.index,
                sar_fc.values, ci.iloc[:, 0].values, ci.iloc[:, 1].values)
    resid = float(np.std(train.values - ets.fittedvalues.values))
    return ("ETS", ets_mae, ets, test.index, ets_fc.values,
            ets_fc.values - 1.96 * resid, ets_fc.values + 1.96 * resid)


def main():
    os.makedirs(MODELS_DIR, exist_ok=True)
    conn = connect()
    series = load_series(conn)
    print(f"[zones] forecasting top {len(series)} by total pickups")

    rows, summary = [], []
    for z, s in series.items():
        try:
            name, mae, model, idx, fc, lo, hi = fit_eval(s)
        except Exception as e:
            print(f"[zone {z}] FAILED: {e}")
            continue
        lo = np.clip(lo, 0, None)
        fc = np.clip(fc, 0, None)
        for t, p, l, h in zip(idx, fc, lo, hi):
            rows.append((t.strftime("%Y-%m-%d %H:%M:%S"), int(z),
                         float(p), float(l), float(max(h, p)), name, mae))
        if name == "SARIMAX":
            artifact = {"type": "SARIMAX", "order": SARIMAX_ORDER,
                        "seasonal_order": SARIMAX_SEASONAL,
                        "params": np.asarray(model.params)}
        else:
            artifact = model
        path = os.path.join(MODELS_DIR, f"zone_{z}_{name.lower()}.pkl")
        with open(path, "wb") as f:
            pickle.dump(artifact, f)
        summary.append((z, name, mae, os.path.getsize(path)))
        print(f"[zone {z}] {name}  MAE={mae:.2f}  pickle={os.path.getsize(path)/1024:.0f}KB")

    with conn.cursor() as cur:
        cur.execute("TRUNCATE forecast_results")
        execute_values(cur,
            """INSERT INTO forecast_results (forecast_time, zone_id, predicted_demand,
               lower_bound, upper_bound, model_name, mae) VALUES %s""", rows)
    conn.commit()
    conn.close()

    won = pd.Series([s[1] for s in summary]).value_counts().to_dict()
    big = max((s[3] for s in summary), default=0) / 1024
    print(f"\n[done] {len(rows)} forecast rows; model wins={won}; "
          f"largest pickle={big:.0f}KB (was ~49000KB with pmdarima)")


if __name__ == "__main__":
    main()
