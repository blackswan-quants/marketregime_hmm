"""
Unified DataManager class for all data loading, fetching, cleaning, and validation.
"""
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Union, List
from calendar import monthrange
from pathlib import Path

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
import requests
import yfinance as yf

US_BD = CustomBusinessDay(calendar=USFederalHolidayCalendar())
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_SERIES = {
    "DGS10": "10Y Treasury Yield",
    "DGS2": "2Y Treasury Yield",
    "BAA": "BAA Corporate Bond Yield",
    "AAA": "AAA Corporate Bond Yield",
}

class DataManager:
    def __init__(self, data_dir: Optional[Union[str, Path]] = None):
        if data_dir is None:
            self.data_dir = Path(__file__).parent.parent.parent / "data"
        else:
            self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.cleaned_dir = self.data_dir / "cleaned"
        self._ensure_dirs()
        self.data: Dict[str, pd.DataFrame] = {}
        self.merged_data: Optional[pd.DataFrame] = None

    def _ensure_dirs(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.cleaned_dir.mkdir(parents=True, exist_ok=True)

    def _n_months_ago(self, today: datetime, months: int) -> str:
        total_months = today.year * 12 + (today.month - 1)
        target_total = total_months - months
        year = target_total // 12
        month = target_total % 12 + 1
        last_day = monthrange(year, month)[1]
        day = min(today.day, last_day)
        return f"{year:04d}-{month:02d}-{day:02d}"

    def fetch_fred_series(self, api_key: str, series_id: str, start_date: str) -> pd.DataFrame:
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
                sleep_s = backoff ** attempt
                time.sleep(sleep_s)
        raise RuntimeError(
            f"FRED API request failed for {series_id} after {max_retries} attempts. "
            f"Last error: {last_err}"
        )

    def fetch_and_save_fred_data(self, api_key: str, months: int = 12) -> Dict[str, pd.DataFrame]:
        today = datetime.now(timezone.utc)
        start_date = self._n_months_ago(today, months)
        frames: Dict[str, pd.DataFrame] = {}
        for series_id, name in FRED_SERIES.items():
            df = self.fetch_fred_series(api_key, series_id, start_date)
            self._save_csv(df, series_id)
            frames[series_id] = df
            self.data[series_id] = df
        if "BAA" in frames and "AAA" in frames:
            cs = self._derive_credit_spread(frames["BAA"], frames["AAA"])
            self._save_csv(cs, "credit_spread_baa_aaa")
            self.data["credit_spread_baa_aaa"] = cs
        if "DGS10" in frames and "DGS2" in frames:
            yc = self._derive_10y_2y_spread(frames["DGS10"], frames["DGS2"])
            self._save_csv(yc, "yield_curve_10y_2y_spread")
            self.data["yield_curve_10y_2y_spread"] = yc
        return frames

    def _save_csv(self, df: pd.DataFrame, name: str) -> None:
        path = self.raw_dir / f"{name}.csv"
        df.to_csv(path, index=False)

    def _derive_credit_spread(self, baa: pd.DataFrame, aaa: pd.DataFrame) -> pd.DataFrame:
        df = pd.merge(baa, aaa, on="date", how="outer", suffixes=("_baa", "_aaa"))
        df = df.sort_values("date").reset_index(drop=True)
        df["value"] = df["value_baa"] - df["value_aaa"]
        return df[["date", "value"]]

    def _derive_10y_2y_spread(self, d10: pd.DataFrame, d2: pd.DataFrame) -> pd.DataFrame:
        df = pd.merge(d10, d2, on="date", how="outer", suffixes=("_10y", "_2y"))
        df = df.sort_values("date").reset_index(drop=True)
        df["value"] = df["value_10y"] - df["value_2y"]
        return df[["date", "value"]]

    def load_csv(self, filename: str, name: Optional[str] = None) -> pd.DataFrame:
        if not filename.endswith(".csv"):
            filename += ".csv"
        filepath = self.raw_dir / filename
        df = pd.read_csv(filepath)
        store_name = name or Path(filename).stem
        self.data[store_name] = df
        return df

    def forward_fill_missing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.ffill()

    def percent_to_decimal(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        for col in columns:
            df[col] = df[col] / 100.0
            df[col] = df[col].round(7)
        return df

    def check_anomalies_macroeconomic(self, df: pd.DataFrame) -> pd.DataFrame:
        if (df["value"] < 0).any():
            raise ValueError("Anomaly detected: Negative values found in 'value' column.")
        if (df["date"].duplicated()).any():
            raise ValueError("Anomaly detected: Duplicate rows found.")
        if df.isnull().values.any():
            raise ValueError("Anomaly detected: Null values found after cleaning.")
        return df

    def check_time_gaps(self, df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"]).dt.normalize()
        d = d.sort_values("date")
        expected = pd.date_range(d["date"].min(), d["date"].max(), freq=US_BD)
        if not expected.equals(pd.DatetimeIndex(d["date"])):
            missing = expected.difference(pd.DatetimeIndex(d["date"]))
            if len(missing):
                raise ValueError(f"Anomaly: missing {len(missing)} business dates, e.g. {missing[:5].tolist()}")
        return df

    def create_rows_for_missing_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize().dt.tz_localize(None)
        df = df.set_index("date").sort_index()
        bday_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=US_BD)
        df = df.reindex(bday_index)
        return df

    def group_by_date(self, df: pd.DataFrame) -> pd.DataFrame:
        df["date"] = pd.to_datetime(df["date"])
        df["date"] = df["date"].dt.date
        df = (
            df.groupby("date")
            .agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            })
            .reset_index()
        )
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        return df

    def missing_dates(self, df: pd.DataFrame) -> List:
        df = df.copy()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize().dt.tz_localize(None)
            df = df.set_index("date").sort_index()
        bday_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=US_BD)
        missing = bday_index.difference(df.index)
        return missing.tolist()

    def fetch_yahoo_data(self, tickers: List[str], start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        data = yf.download(
            tickers,
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=True,
            threads=False
        )
        result = {}
        for ticker in tickers:
            try:
                df = data.xs(ticker, level=1, axis=1)
                result[ticker] = df
            except Exception:
                result[ticker] = None
        return result
