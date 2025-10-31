#!/usr/bin/env python3

# Fetch the latest 12 months of macro data from FRED API and save CSVs under data/raw/.

# Forward filling and data processing will later be performed

import argparse
import os
import sys
import time
import math
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import requests
import pandas as pd

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "DGS10": "10Y Treasury Yield",
    "DGS2":  "2Y Treasury Yield",
    "BAA":   "BAA Corporate Bond Yield",
    "AAA":   "AAA Corporate Bond Yield",
}

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")

# create data/raw dir
def _ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)

# calculate 365 days before a given date
def _twelve_months_ago(today: datetime) -> str:
    start = today - timedelta(days=365)
    return start.strftime("%Y-%m-%d")

def fetch_series(api_key: str, series_id: str, start_date: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
    }
    for attempt in range(3):
        resp = requests.get(FRED_BASE, params=params, timeout=30)
        if resp.status_code == 200:
            payload = resp.json()
            obs = payload.get("observations", [])
            df = pd.DataFrame(obs)
            if df.empty:
                return pd.DataFrame(columns=["date", "value"])
            df = df[["date", "value"]].copy()
            
            df["date"] = pd.to_datetime(df["date"], utc=False).dt.date
            df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
            df = df.sort_values("date").reset_index(drop=True)
            return df
        else:
            time.sleep(1 + attempt)
    raise RuntimeError(f"FRED API request failed for {series_id}: HTTP {resp.status_code} - {resp.text[:200]}")

# FRED API only allows for zipped .csv return so I fetched data in .json and then wrote a .csv
def save_csv(df: pd.DataFrame, name: str):
    path = os.path.join(RAW_DIR, f"{name}.csv")
    df.to_csv(path, index=False)

def derive_credit_spread(baa: pd.DataFrame, aaa: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(baa, aaa, on="date", how="outer", suffixes=("_baa", "_aaa"))
    df = df.sort_values("date").reset_index(drop=True)
    df["value"] = df["value_baa"] - df["value_aaa"]
    return df[["date", "value"]]

def derive_10y_2y_spread(d10: pd.DataFrame, d2: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(d10, d2, on="date", how="outer", suffixes=("_10y", "_2y"))
    df = df.sort_values("date").reset_index(drop=True)
    df["value"] = df["value_10y"] - df["value_2y"]
    return df[["date", "value"]]


def fetch_and_save(api_key: str):
    _ensure_dirs()
    today = datetime.utcnow()
    start_date = _twelve_months_ago(today)

    print(f"[FRED] fetching last 12 months starting {start_date} ...")

    frames: Dict[str, pd.DataFrame] = {}
    for series_id, name in SERIES.items():
        print(f" - {series_id}: {name}")
        df = fetch_series(api_key, series_id, start_date)
        save_csv(df, series_id)
        frames[series_id] = df

    # Derived: credit spread (BAA - AAA)
    if "BAA" in frames and "AAA" in frames:
        cs = derive_credit_spread(frames["BAA"], frames["AAA"])
        save_csv(cs, "credit_spread_baa_aaa")
        print(" - credit_spread_baa_aaa: saved.")

    # Derived: yield curve 10Y-2Y
    if "DGS10" in frames and "DGS2" in frames:
        yc = derive_10y_2y_spread(frames["DGS10"], frames["DGS2"])
        save_csv(yc, "yield_curve_10y_2y_spread")
        print(" - yield_curve_10y_2y_spread: saved.")


    print("Done. Raw CSVs available in data/raw/.")

def main():
    parser = argparse.ArgumentParser(description="Fetch last 12 months of macro data from FRED and save raw CSVs.")
    parser.add_argument("--api-key", type=str, default=os.getenv("FRED_API_KEY"), help="FRED API key (or set FRED_API_KEY env var).")
    args = parser.parse_args()

    if not args.api_key:
        print("ERROR: FRED API key is required. Use --api-key or set FRED_API_KEY env var.", file=sys.stderr)
        sys.exit(1)

    fetch_and_save(args.api_key)

if __name__ == "__main__":
    main()
