# Fetch the latest n months of macro data from FRED API
# and save raw CSVs under data/raw/
#
# Forward filling and data processing will later be performed
#
# import argparse
# import os
# import sys
# import time
# from calendar import monthrange
# from datetime import datetime, timezone, timedelta
# from typing import Dict
#
# import pandas as pd
# import requests
# import yfinance as yf
#
# FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
#
# SERIES = {
#     "DGS10": "10Y Treasury Yield",
#     "DGS2": "2Y Treasury Yield",
#     "BAA": "BAA Corporate Bond Yield",
#     "AAA": "AAA Corporate Bond Yield",
# }
#
# # Default months fetch is 12, can be overriden via --months cmd flag
# DEFAULT_MONTH_SPAN = 12
#
# RAW_DIR = "data/raw"
#
#
# # create data/raw dir
# def _ensure_dirs():
#     """Create data/raw directory if it doesn't exist."""
#     os.makedirs(RAW_DIR, exist_ok=True)
#
#
# # calculate n months before today's date
# def _n_months_ago(today: datetime, months: int) -> str:
#     """Calculate date N months before given date.
#
#     Args:
#         today: Reference date.
#         months: Number of months to subtract.
#
#     Returns:
#         Date string in YYYY-MM-DD format.
#     """
#     total_months = today.year * 12 + (today.month - 1)
#     target_total = total_months - months
#     year = target_total // 12
#     month = target_total % 12 + 1
#     last_day = monthrange(year, month)[1]
#     day = min(today.day, last_day)
#     return f"{year:04d}-{month:02d}-{day:02d}"
#
#
# def fetch_series(api_key: str, series_id: str, start_date: str) -> pd.DataFrame:
#     """Fetch time series data from FRED API.
#
#     Args:
#         api_key: FRED API key.
#         series_id: FRED series identifier.
#         start_date: Start date in YYYY-MM-DD format.
#
#     Returns:
#         DataFrame with date and value columns.
#
#     Raises:
#         RuntimeError: If API request fails after retries.
#     """
#     params = {
#         "series_id": series_id,
#         "api_key": api_key,
#         "file_type": "json",
#         "observation_start": start_date,
#     }
#
#     max_retries = 3
#     backoff = 1.5
#     attempt = 0
#     last_err = None
#
#     while attempt < max_retries:
#         try:
#             resp = requests.get(FRED_BASE, params=params, timeout=30)
#             resp.raise_for_status()
#             payload = resp.json()
#             obs = payload.get("observations", [])
#             df = pd.DataFrame(obs)
#             if df.empty:
#                 return pd.DataFrame(columns=["date", "value"])
#
#             df = df[["date", "value"]].copy()
#             df["date"] = pd.to_datetime(df["date"], utc=False).dt.date
#             df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
#             df = df.sort_values("date").reset_index(drop=True)
#             return df
#
#         except (requests.exceptions.RequestException, ValueError) as e:
#             last_err = e
#             attempt += 1
#             if attempt >= max_retries:
#                 break
#
#             sleep_s = backoff**attempt
#             time.sleep(sleep_s)
#
#     raise RuntimeError(
#         f"FRED API request failed for {series_id} after {max_retries} attempts. " f"Last error: {last_err}"
#     )
#
#
# def save_csv(df: pd.DataFrame, name: str):
#     """Save DataFrame to CSV in data/raw directory.
#
#     Args:
#         df: DataFrame to save.
#         name: Filename without extension.
#     """
#     path = os.path.join(RAW_DIR, f"{name}.csv")
#     df.to_csv(path, index=False)
#
# def derive_credit_spread(baa: pd.DataFrame, aaa: pd.DataFrame) -> pd.DataFrame:
#     """Calculate credit spread between BAA and AAA yields.
#
#     Args:
#         baa: BAA yield data.
#         aaa: AAA yield data.
#
#     Returns:
#         DataFrame with credit spread.
#     """
#     df = pd.merge(baa, aaa, on="date", how="outer", suffixes=("_baa", "_aaa"))
#     df = df.sort_values("date").reset_index(drop=True)
#     df["value"] = df["value_baa"] - df["value_aaa"]
#     return df[["date", "value"]]
#
#
# def derive_10y_2y_spread(d10: pd.DataFrame, d2: pd.DataFrame) -> pd.DataFrame:
#     """Calculate yield curve spread between 10Y and 2Y treasuries.
#
#     Args:
#         d10: 10Y treasury yield data.
#         d2: 2Y treasury yield data.
#
#     Returns:
#         DataFrame with yield curve spread.
#     """
#     df = pd.merge(d10, d2, on="date", how="outer", suffixes=("_10y", "_2y"))
#     df = df.sort_values("date").reset_index(drop=True)
#     df["value"] = df["value_10y"] - df["value_2y"]
#     return df[["date", "value"]]
#
#
# def fetch_and_save(api_key: str, months: int):
#     """Fetch FRED data for all series and save to CSV.
#
#     Args:
#         api_key: FRED API key.
#         months: Number of months of historical data.
#     """
#     _ensure_dirs()
#     today = datetime.now(timezone.utc)
#     start_date = _n_months_ago(today, months)
#     print(f"[FRED] fetching last {months} months starting {start_date} ...")
#
#     frames: Dict[str, pd.DataFrame] = {}
#     for series_id, name in SERIES.items():
#         print(f" - {series_id}: {name}")
#         df = fetch_series(api_key, series_id, start_date)
#         save_csv(df, series_id)
#         frames[series_id] = df
#
#     if "BAA" in frames and "AAA" in frames:
#         cs = derive_credit_spread(frames["BAA"], frames["AAA"])
#         save_csv(cs, "credit_spread_baa_aaa")
#         print(" - credit_spread_baa_aaa: saved.")
#
#     if "DGS10" in frames and "DGS2" in frames:
#         yc = derive_10y_2y_spread(frames["DGS10"], frames["DGS2"])
#         save_csv(yc, "yield_curve_10y_2y_spread")
#         print(" - yield_curve_10y_2y_spread: saved.")
#
#     print("Done. Raw CSVs available in data/raw/.")
#
#
# def main():
#     parser = argparse.ArgumentParser(description="Fetch last N months of macro data from FRED and save raw CSVs.")
#     parser.add_argument(
#         "--api-key",
#         type=str,
#         default=os.getenv("FRED_API_KEY"),
#         help="FRED API key (or set FRED_API_KEY env var).",
#     )
#     parser.add_argument(
#         "--months",
#         type=int,
#         default=DEFAULT_MONTH_SPAN,
#         help=f"Number of months to fetch (default {DEFAULT_MONTH_SPAN}).",
#     )
#     args = parser.parse_args()
#
#     if not args.api_key:
#         print(
#             "ERROR: FRED API key is required. " "Use --api-key or set FRED_API_KEY env var.",
#             file=sys.stderr,
#         )
#         sys.exit(1)
#     if args.months <= 0:
#         print("ERROR: --months must be a positive integer.", file=sys.stderr)
#         sys.exit(1)
#
#     fetch_and_save(args.api_key, args.months)
#
#
#     tickers = ["MOVE", "TLT"]
#     start_date = _n_months_ago(datetime.strptime("2025-11-01", "%Y-%m-%d"), DEFAULT_MONTH_SPAN)
#     end_date = datetime.now(timezone.utc)
#
#     data = yf.download(
#         tickers,
#         start=start_date,
#         end=end_date + timedelta(days=1),
#         progress=False,
#         auto_adjust=True,
#         threads=False  # It avoids database lock issues
#     )
#
#     #Save MOVE data
#     move_data = data.xs('MOVE', level=1, axis=1)
#     move_data.to_csv(os.path.join(RAW_DIR, "MOVE.csv"))
#
#     #Save TLT data
#     tlt_data = data.xs('TLT', level=1, axis=1)
#     tlt_data.to_csv(os.path.join(RAW_DIR, "TLT.csv"))
#
#
# if __name__ == "__main__":
#     main()
#!/usr/bin/env python3

# Fetch the latest n months of macro data from FRED API
# and save raw CSVs under data/raw/

# Forward filling and data processing will later be performed

import argparse
import os
import sys
import time
from calendar import monthrange
from datetime import datetime, timedelta, timezone
from typing import Dict

import pandas as pd
import requests
import yfinance as yf

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "DGS10": "10Y Treasury Yield",
    "DGS2": "2Y Treasury Yield",
    "BAA": "BAA Corporate Bond Yield",
    "AAA": "AAA Corporate Bond Yield",
}

# Default months fetch is 12, can be overriden via --months cmd flag
DEFAULT_MONTH_SPAN = 12

RAW_DIR = "data/raw"


# create data/raw dir
def _ensure_dirs():
    """Create data/raw directory if it doesn't exist."""
    os.makedirs(RAW_DIR, exist_ok=True)


# calculate n months before today's date
def _n_months_ago(today: datetime, months: int) -> str:
    """Calculate date N months before given date.

    Args:
        today: Reference date.
        months: Number of months to subtract.

    Returns:
        Date string in YYYY-MM-DD format.
    """
    total_months = today.year * 12 + (today.month - 1)
    target_total = total_months - months
    year = target_total // 12
    month = target_total % 12 + 1
    last_day = monthrange(year, month)[1]
    day = min(today.day, last_day)
    return f"{year:04d}-{month:02d}-{day:02d}"


def fetch_series(api_key: str, series_id: str, start_date: str) -> pd.DataFrame:
    """Fetch time series data from FRED API.

    Args:
        api_key: FRED API key.
        series_id: FRED series identifier.
        start_date: Start date in YYYY-MM-DD format.

    Returns:
        DataFrame with date and value columns.

    Raises:
        RuntimeError: If API request fails after retries.
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
    }

    max_retries = 3
    backoff = 1.5
    attempt = 0
    last_err = None

    while attempt < max_retries:
        try:
            resp = requests.get(FRED_BASE, params=params, timeout=30)
            resp.raise_for_status()
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

        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = e
            attempt += 1
            if attempt >= max_retries:
                break

            sleep_s = backoff**attempt
            time.sleep(sleep_s)

    raise RuntimeError(
        f"FRED API request failed for {series_id} after {max_retries} attempts. " f"Last error: {last_err}"
    )


def save_csv(df: pd.DataFrame, name: str):
    """Save DataFrame to CSV in data/raw directory.

    Args:
        df: DataFrame to save.
        name: Filename without extension.
    """
    path = os.path.join(RAW_DIR, f"{name}.csv")
    df.to_csv(path, index=False)


def derive_credit_spread(baa: pd.DataFrame, aaa: pd.DataFrame) -> pd.DataFrame:
    """Calculate credit spread between BAA and AAA yields.

    Args:
        baa: BAA yield data.
        aaa: AAA yield data.

    Returns:
        DataFrame with credit spread.
    """
    df = pd.merge(baa, aaa, on="date", how="outer", suffixes=("_baa", "_aaa"))
    df = df.sort_values("date").reset_index(drop=True)
    df["value"] = df["value_baa"] - df["value_aaa"]
    return df[["date", "value"]]


def derive_10y_2y_spread(d10: pd.DataFrame, d2: pd.DataFrame) -> pd.DataFrame:
    """Calculate yield curve spread between 10Y and 2Y treasuries.

    Args:
        d10: 10Y treasury yield data.
        d2: 2Y treasury yield data.

    Returns:
        DataFrame with yield curve spread.
    """
    df = pd.merge(d10, d2, on="date", how="outer", suffixes=("_10y", "_2y"))
    df = df.sort_values("date").reset_index(drop=True)
    df["value"] = df["value_10y"] - df["value_2y"]
    return df[["date", "value"]]


def fetch_and_save(api_key: str, months: int):
    """Fetch FRED data for all series and save to CSV.

    Args:
        api_key: FRED API key.
        months: Number of months of historical data.
    """
    _ensure_dirs()
    today = datetime.now(timezone.utc)
    start_date = _n_months_ago(today, months)
    print(f"[FRED] fetching last {months} months starting {start_date} ...")

    frames: Dict[str, pd.DataFrame] = {}
    for series_id, name in SERIES.items():
        print(f" - {series_id}: {name}")
        df = fetch_series(api_key, series_id, start_date)
        save_csv(df, series_id)
        frames[series_id] = df

    if "BAA" in frames and "AAA" in frames:
        cs = derive_credit_spread(frames["BAA"], frames["AAA"])
        save_csv(cs, "credit_spread_baa_aaa")
        print(" - credit_spread_baa_aaa: saved.")

    if "DGS10" in frames and "DGS2" in frames:
        yc = derive_10y_2y_spread(frames["DGS10"], frames["DGS2"])
        save_csv(yc, "yield_curve_10y_2y_spread")
        print(" - yield_curve_10y_2y_spread: saved.")

    print("Done. Raw CSVs available in data/raw/.")


def main():
    parser = argparse.ArgumentParser(description="Fetch last N months of macro data from FRED and save raw CSVs.")
    parser.add_argument(
        "--api-key",
        type=str,
        default=os.getenv("FRED_API_KEY"),
        help="FRED API key (or set FRED_API_KEY env var).",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=DEFAULT_MONTH_SPAN,
        help=f"Number of months to fetch (default {DEFAULT_MONTH_SPAN}).",
    )
    args = parser.parse_args()

    if not args.api_key:
        print(
            "ERROR: FRED API key is required. " "Use --api-key or set FRED_API_KEY env var.",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.months <= 0:
        print("ERROR: --months must be a positive integer.", file=sys.stderr)
        sys.exit(1)

    fetch_and_save(args.api_key, args.months)

    tickers = ["MOVE", "TLT"]
    start_date = _n_months_ago(datetime.strptime("2025-11-01", "%Y-%m-%d"), DEFAULT_MONTH_SPAN)
    end_date = datetime.now(timezone.utc)

    data = yf.download(
        tickers,
        start=start_date,
        end=end_date + timedelta(days=1),
        progress=False,
        auto_adjust=True,
        threads=False,  # It avoids database lock issues
    )

    # Save MOVE data
    move_data = data.xs("MOVE", level=1, axis=1)
    move_data.to_csv(os.path.join(RAW_DIR, "MOVE.csv"))

    # Save TLT data
    tlt_data = data.xs("TLT", level=1, axis=1)
    tlt_data.to_csv(os.path.join(RAW_DIR, "TLT.csv"))


if __name__ == "__main__":
    main()
