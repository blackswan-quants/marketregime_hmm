# This file has been removed as its logic will be replaced by the unified DataManager class.
"""
Unified Data Manager for market and macro data loading, fetching, and cleaning.

This module consolidates all data operations (fetching, loading, and cleaning)
from the data team into a single DataManager class.
"""

import os
import sys
import time
from calendar import monthrange
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd  # type: ignore
import requests  # type: ignore
from pandas.tseries.holiday import USFederalHolidayCalendar  # type: ignore
from pandas.tseries.offsets import CustomBusinessDay  # type: ignore

# Configure US business days (excluding federal holidays)
US_BD = CustomBusinessDay(calendar=USFederalHolidayCalendar())

# FRED API configuration
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_SERIES = {
    "DGS10": "10Y Treasury Yield",
    "DGS2": "2Y Treasury Yield",
    "BAA": "BAA Corporate Bond Yield",
    "AAA": "AAA Corporate Bond Yield",
}


class DataManager:
    """
    Unified data management class for fetching, loading, cleaning, and processing
    market and macroeconomic data.

    Combines functionality from:
    - fetch_data.py: FRED API data fetching
    - clean_data.py: Data cleaning and validation
    - loader.py: Data loading from various formats
    """

    def __init__(self, data_dir: Optional[Union[str, Path]] = None):
        """
        Initialize the DataManager.

        Args:
            data_dir: Path to the data directory. Defaults to 'data' folder in project root.
        """
        if data_dir is None:
            # Default to project root/data
            self.data_dir = Path(__file__).parent.parent.parent / "data"
        else:
            self.data_dir = Path(data_dir)

        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self._ensure_dirs()

        self.data: Dict[str, pd.DataFrame] = {}
        self.merged_data: Optional[pd.DataFrame] = None

    def _ensure_dirs(self) -> None:
        """Ensure data directories exist."""
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    # ==================== FRED API Fetching Methods ====================

    def _n_months_ago(self, today: datetime, months: int) -> str:
        """Calculate a date n months before today."""
        total_months = today.year * 12 + (today.month - 1)
        target_total = total_months - months
        year = target_total // 12
        month = target_total % 12 + 1
        last_day = monthrange(year, month)[1]
        day = min(today.day, last_day)
        return f"{year:04d}-{month:02d}-{day:02d}"

    def fetch_fred_series(self, api_key: str, series_id: str, start_date: str) -> pd.DataFrame:
        """
        Fetch a single FRED series from the API.

        Args:
            api_key: FRED API key
            series_id: Series ID (e.g., 'DGS10', 'BAA')
            start_date: Start date in YYYY-MM-DD format

        Returns:
            DataFrame with 'date' and 'value' columns

        Raises:
            RuntimeError: If the API request fails after retries
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
                df["date"] = pd.to_datetime(df["date"], utc=False).dt.date  # type: ignore[attr-defined]
                df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
                df = df.sort_values("date").reset_index(drop=True)  # type: ignore[call-overload]
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

    def fetch_and_save_fred_data(self, api_key: str, months: int = 12) -> Dict[str, pd.DataFrame]:
        """
        Fetch macroeconomic data from FRED API and save as raw CSVs.

        Args:
            api_key: FRED API key (or set FRED_API_KEY environment variable)
            months: Number of months to fetch (default 12)

        Returns:
            Dictionary of fetched DataFrames
        """
        today = datetime.now(timezone.utc)
        start_date = self._n_months_ago(today, months)
        print(f"[FRED] Fetching last {months} months starting {start_date} ...")

        frames: Dict[str, pd.DataFrame] = {}
        for series_id, name in FRED_SERIES.items():
            print(f"  - {series_id}: {name}")
            df = self.fetch_fred_series(api_key, series_id, start_date)
            self._save_csv(df, series_id)
            frames[series_id] = df
            self.data[series_id] = df

        # Derive credit spread
        if "BAA" in frames and "AAA" in frames:
            cs = self._derive_credit_spread(frames["BAA"], frames["AAA"])
            self._save_csv(cs, "credit_spread_baa_aaa")
            self.data["credit_spread_baa_aaa"] = cs
            print("  - credit_spread_baa_aaa: derived and saved")

        # Derive 10Y-2Y spread
        if "DGS10" in frames and "DGS2" in frames:
            yc = self._derive_10y_2y_spread(frames["DGS10"], frames["DGS2"])
            self._save_csv(yc, "yield_curve_10y_2y_spread")
            self.data["yield_curve_10y_2y_spread"] = yc
            print("  - yield_curve_10y_2y_spread: derived and saved")

        print("Done. Raw CSVs saved to data/raw/")
        return frames

    def _save_csv(self, df: pd.DataFrame, name: str) -> None:
        """Save DataFrame to CSV in raw directory."""
        path = self.raw_dir / f"{name}.csv"
        df.to_csv(path, index=False)

    def _derive_credit_spread(self, baa: pd.DataFrame, aaa: pd.DataFrame) -> pd.DataFrame:
        """Derive BAA-AAA credit spread."""
        df = pd.merge(baa, aaa, on="date", how="outer", suffixes=("_baa", "_aaa"))
        df = df.sort_values("date").reset_index(drop=True)
        df["value"] = df["value_baa"] - df["value_aaa"]
        result: pd.DataFrame = df[["date", "value"]]  # type: ignore[assignment]
        return result

    def _derive_10y_2y_spread(self, d10: pd.DataFrame, d2: pd.DataFrame) -> pd.DataFrame:
        """Derive 10Y-2Y yield curve spread."""
        df = pd.merge(d10, d2, on="date", how="outer", suffixes=("_10y", "_2y"))
        df = df.sort_values("date").reset_index(drop=True)
        df["value"] = df["value_10y"] - df["value_2y"]
        result: pd.DataFrame = df[["date", "value"]]  # type: ignore[assignment]
        return result

    # ==================== Data Loading Methods ====================

    def load_csv(self, filename: str, name: Optional[str] = None) -> pd.DataFrame:
        """
        Load CSV file from raw directory.

        Args:
            filename: Name of the CSV file (with or without .csv extension)
            name: Name to store in data dict. Defaults to filename stem.

        Returns:
            Loaded DataFrame
        """
        if not filename.endswith(".csv"):
            filename += ".csv"

        filepath = self.raw_dir / filename
        df = pd.read_csv(filepath)

        store_name = name or Path(filename).stem
        self.data[store_name] = df
        return df

    def load_all_raw_data(self) -> Dict[str, pd.DataFrame]:
        """Load all CSV files from raw directory."""
        for filepath in self.raw_dir.glob("*.csv"):
            df = pd.read_csv(filepath)
            self.data[filepath.stem] = df
        return self.data

    # ==================== Data Cleaning Methods ====================

    def forward_fill_missing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Forward fill missing values."""
        return df.ffill()

    def percent_to_decimal(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """Convert percentage columns to decimal format."""
        for col in columns:
            df[col] = df[col] / 100.0
            df[col] = df[col].round(7)
        return df

    def check_anomalies_macroeconomic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check for anomalies in macroeconomic data.

        Raises:
            ValueError: If anomalies are detected
        """
        if (df["value"] < 0).any():
            raise ValueError("Anomaly detected: Negative values found in 'value' column.")

        if (df["date"].duplicated()).any():
            raise ValueError("Anomaly detected: Duplicate rows found.")

        if df.isnull().values.any():
            raise ValueError("Anomaly detected: Null values found after cleaning.")

        return df

    def check_time_gaps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check for missing business days in time series data.

        Raises:
            ValueError: If business days are missing
        """
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"]).dt.normalize()  # type: ignore[attr-defined]
        d = d.sort_values("date")
        expected = pd.date_range(d["date"].min(), d["date"].max(), freq=US_BD)

        if not expected.equals(pd.DatetimeIndex(d["date"])):
            missing = expected.difference(pd.DatetimeIndex(d["date"]))
            if len(missing):
                raise ValueError(f"Anomaly: missing {len(missing)} business dates, e.g. {missing[:5].tolist()}")

        return df

    def create_rows_for_missing_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create rows for missing business dates and forward fill.

        Args:
            df: DataFrame with 'date' column

        Returns:
            DataFrame with complete business day index
        """
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize().dt.tz_localize(None)  # type: ignore[attr-defined]
        df = df.set_index("date").sort_index()

        bday_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=US_BD)
        df = df.reindex(bday_index)

        return df

    def group_by_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Group intraday data by date (OHLCV aggregation).

        Args:
            df: DataFrame with 'date', 'open', 'high', 'low', 'close', 'volume' columns

        Returns:
            Daily aggregated DataFrame with date as index
        """
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df["date"] = df["date"].dt.date

        df = (
            df.groupby("date")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .reset_index()
        )

        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        return df

    def missing_dates(self, df: pd.DataFrame) -> List:
        """
        Identify missing business dates in a time series.

        Args:
            df: DataFrame with 'date' column or datetime index

        Returns:
            List of missing business dates
        """
        df = df.copy()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize().dt.tz_localize(None)  # type: ignore[attr-defined]
            df = df.set_index("date").sort_index()

        bday_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=US_BD)
        missing = bday_index.difference(df.index)  # type: ignore[arg-type]

        return missing.tolist()

    # ==================== Data Processing Pipeline ====================

    def clean_macro_data(self, df: pd.DataFrame, name: str, check_gaps: bool = False) -> pd.DataFrame:
        """
        Clean macroeconomic data (BAA, AAA, yield spreads, credit spreads).

        Args:
            df: Raw DataFrame
            name: Name of the dataset (for logging)
            check_gaps: Whether to check for time gaps

        Returns:
            Cleaned DataFrame with datetime index
        """
        print(f"Cleaning {name}...")

        df = self.forward_fill_missing_data(df)

        if check_gaps:
            df = self.check_time_gaps(df)

        df = self.percent_to_decimal(df, ["value"])
        df = self.check_anomalies_macroeconomic(df)

        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df = df.rename(columns={"value": name})

        return df

    def clean_price_data(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """
        Clean price data (SPY, VIX) to daily OHLCV.

        Args:
            df: Raw intraday DataFrame
            name: Name of the dataset (SPY, VIX, etc.)

        Returns:
            Cleaned daily DataFrame with datetime index
        """
        print(f"Cleaning {name}...")

        df = df.copy()
        if "caldt" in df.columns:
            df = df.rename(columns={"caldt": "date"})

        df_subset: pd.DataFrame = df[["date", "open", "high", "low", "close", "volume"]]  # type: ignore[assignment]
        df = df_subset
        df = self.group_by_date(df)
        df.reset_index(inplace=True)

        # Create rows for missing dates
        df = self.create_rows_for_missing_dates(df)

        # Rename columns with ticker suffix
        df.columns = [f"{col}_{name}" if col != name else col for col in df.columns]

        return df

    def merge_market_and_macro_data(self) -> pd.DataFrame:
        """
        Master pipeline: Load, clean, and merge all market and macro data.

        Returns:
            Merged DataFrame with all market and macro indicators
        """
        print("\n=== Loading and Cleaning Market and Macro Data ===\n")

        # Load credit spread
        print("Processing credit spread...")
        spread_baa_aaa = self.load_csv("credit_spread_baa_aaa.csv")
        spread_baa_aaa = self.clean_macro_data(spread_baa_aaa, "credit_spread_baa_aaa")

        # Load yield curve spread
        print("Processing yield curve spread...")
        curve_10y_2y = self.load_csv("yield_curve_10y_2y_spread.csv")
        curve_10y_2y = self.clean_macro_data(curve_10y_2y, "yield_curve_10y2y", check_gaps=True)

        # Load and clean SPY
        print("Processing SPY...")
        spy = self.load_csv("SPY_1min_20231027_20251027.csv")
        spy = self.clean_price_data(spy, "SPY")
        spy_missing = self.missing_dates(spy)

        # Load and clean VIX
        print("Processing VIX...")
        vix = self.load_csv("^VIX_1day_20231027_20251027.csv")
        vix = self.clean_price_data(vix, "VIX")

        # Merge all datasets
        print("\nMerging all datasets...")
        df_final = spy.join(vix, lsuffix="_SPY", rsuffix="_VIX", how="outer")
        df_final = df_final.join(curve_10y_2y, how="outer")
        df_final = df_final.join(spread_baa_aaa, how="outer")

        # Remove identified missing dates and forward fill
        df_final = df_final.drop(spy_missing, axis=0, errors="ignore")
        df_final = df_final.ffill()
        df_final = df_final.dropna()

        self.merged_data = df_final
        print(f"Merged data shape: {df_final.shape}")

        return df_final

    def save_processed_data(self, df: Optional[pd.DataFrame] = None) -> None:
        """
        Save processed data to CSV and Parquet.

        Args:
            df: DataFrame to save. Defaults to self.merged_data.
        """
        if df is None:
            df = self.merged_data

        if df is None:
            raise ValueError("No data to save. Run merge_market_and_macro_data() first.")

        csv_path = self.processed_dir / "market_macro_merged.csv"
        parquet_path = self.processed_dir / "market_macro_merged.parquet"

        df.to_csv(csv_path, index=True, index_label="date")
        df.to_parquet(parquet_path, index=True)

        print(f"Processed data saved to:")
        print(f"  - {csv_path}")
        print(f"  - {parquet_path}")

    def get_processed_data(self) -> Optional[pd.DataFrame]:
        """Get the current merged/processed data."""
        return self.merged_data

    def get_data_info(self, df: Optional[pd.DataFrame] = None) -> Dict:
        """Get information about a DataFrame."""
        if df is None:
            df = self.merged_data

        if df is None:
            raise ValueError("No data available.")

        return {
            "shape": df.shape,
            "columns": df.columns.tolist(),
            "dtypes": df.dtypes.to_dict(),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024**2,
            "date_range": (df.index.min(), df.index.max()) if hasattr(df.index, "min") else "N/A",
        }


if __name__ == "__main__":
    # Example usage
    manager = DataManager()

    # Option 1: Fetch fresh data from FRED
    # api_key = os.getenv("FRED_API_KEY")
    # if api_key:
    #     manager.fetch_and_save_fred_data(api_key, months=12)

    # Option 2: Use existing data and run full pipeline
    merged_df = manager.merge_market_and_macro_data()
    manager.save_processed_data()

    print("\nData Info:")
    print(manager.get_data_info())
