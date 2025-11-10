import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
import os

CLEANED_DIR = "data/cleaned"
US_BD = CustomBusinessDay(calendar=USFederalHolidayCalendar())  # FRED/US markets


def forward_fill_missing_data(df: pd.DataFrame):
    """Forward fill missing values in DataFrame.

    Args:
        df: DataFrame to fill.

    Returns:
        DataFrame with forward filled values.
    """
    return df.ffill()


def percent_to_decimal(df: pd.DataFrame, columns: list[str]):
    """Convert percentage columns to decimal format.

    Args:
        df: DataFrame to modify.
        columns: Column names to convert.

    Returns:
        DataFrame with converted columns.
    """
    for col in columns:
        df[col] = df[col] / 100.0
        df[col] = df[col].round(7)
    return df


def check_anomalies_macroeconomic(df: pd.DataFrame):
    """Validate macroeconomic data for anomalies.

    Args:
        df: DataFrame to validate.

    Returns:
        Validated DataFrame.

    Raises:
        ValueError: If anomalies detected.
    """
    # Ensure no negative values in 'value' column
    if (df["value"] < 0).any():
        raise ValueError("Anomaly detected: Negative values found in 'value' column.")

    # Ensure no duplicate rows
    if (df["date"].duplicated()).any():
        raise ValueError("Anomaly detected: Duplicate rows found.")

    # Ensure that there are no more null values
    if df.isnull().values.any():
        raise ValueError("Anomaly detected: Null values found after cleaning.")

    return df


def check_time_gaps(df: pd.DataFrame):
    """Check for missing business days in time series.

    Args:
        df: DataFrame with date column.

    Returns:
        Validated DataFrame.

    Raises:
        ValueError: If gaps detected.
    """
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"]).dt.normalize()
    d = d.sort_values("date")
    expected = pd.date_range(d["date"].min(), d["date"].max(), freq=US_BD)
    if not expected.equals(pd.DatetimeIndex(d["date"])):
        missing = expected.difference(pd.DatetimeIndex(d["date"]))
        if len(missing):
            raise ValueError(f"Anomaly: missing {len(missing)} business dates, " f"e.g. {missing[:5].tolist()}")

    return df


def create_rows_for_missing_dates(df: pd.DataFrame):
    """Create rows for missing business dates.

    Args:
        df: DataFrame with date column.

    Returns:
        DataFrame with complete business date index.
    """
    # Convert date to datetime, normalize to midnight UTC, then remove timezone
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize().dt.tz_localize(None)
    df = df.set_index("date").sort_index()

    # Create business day index and reindex
    bday_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=US_BD)
    df = df.reindex(bday_index)
    # df = forward_fill_missing_data(df)

    return df


def group_by_date(df: pd.DataFrame):
    """Aggregate intraday data to daily OHLCV bars.

    Args:
        df: DataFrame with OHLCV columns.

    Returns:
        DataFrame grouped by date.
    """
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.date
    df = (
        df.groupby("date")
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .reset_index()
    )
    df["date"] = pd.to_datetime(df["date"])
    # Set date as index for joining
    df = df.set_index("date")
    return df

if __name__ == "__main__":

    # credit_spread_baa_aaa
    spread_baa_aaa = pd.read_csv("data/raw/credit_spread_baa_aaa.csv")
    spread_baa_aaa = forward_fill_missing_data(spread_baa_aaa)
    spread_baa_aaa = percent_to_decimal(spread_baa_aaa, ["value"])
    spread_baa_aaa = check_anomalies_macroeconomic(spread_baa_aaa)
    spread_baa_aaa["date"] = pd.to_datetime(spread_baa_aaa["date"])
    spread_baa_aaa = spread_baa_aaa.set_index("date")
    spread_baa_aaa.to_parquet(os.path.join(CLEANED_DIR, "credit_spread.parquet"))
    spread_baa_aaa.to_csv(os.path.join(CLEANED_DIR, "credit_spread.csv"))
    spread_baa_aaa = spread_baa_aaa.rename(columns={"value": "credit_spread"})

    # DGS2
    DGS2 = pd.read_csv("data/raw/DGS2.csv")
    DGS2 = check_time_gaps(DGS2)
    DGS2 = forward_fill_missing_data(DGS2)
    DGS2 = percent_to_decimal(DGS2, ["value"])
    DGS2 = check_anomalies_macroeconomic(DGS2)
    DGS2["date"] = pd.to_datetime(DGS2["date"])
    DGS2 = DGS2.set_index("date")
    DGS2.to_parquet(os.path.join(CLEANED_DIR, "dgs2.parquet"))
    DGS2.to_csv(os.path.join(CLEANED_DIR, "dgs2.csv"))

    # DGS10
    DGS10 = pd.read_csv("data/raw/DGS10.csv")
    DGS10 = check_time_gaps(DGS10)
    DGS10 = forward_fill_missing_data(DGS10)
    DGS10 = percent_to_decimal(DGS10, ["value"])
    DGS10 = check_anomalies_macroeconomic(DGS10)
    DGS10["date"] = pd.to_datetime(DGS10["date"])
    DGS10 = DGS10.set_index("date")
    DGS10.to_parquet(os.path.join(CLEANED_DIR, "dgs10.parquet"))
    DGS10.to_csv(os.path.join(CLEANED_DIR, "dgs10.csv"))

    # yield_curve_10y_2y_spread
    curve_10y_2y = pd.read_csv("data/raw/yield_curve_10y_2y_spread.csv")
    curve_10y_2y = check_time_gaps(curve_10y_2y)
    curve_10y_2y = forward_fill_missing_data(curve_10y_2y)
    curve_10y_2y = percent_to_decimal(curve_10y_2y, ["value"])
    curve_10y_2y = check_anomalies_macroeconomic(curve_10y_2y)
    curve_10y_2y["date"] = pd.to_datetime(curve_10y_2y["date"])
    curve_10y_2y = curve_10y_2y.set_index("date")
    curve_10y_2y = curve_10y_2y.rename(columns={"value": "yield_curve_10y2y"})

    # SPY
    SPY = pd.read_csv("data/raw/SPY_1min_20231027_20251027.csv")
    SPY = SPY.rename(columns={"caldt": "date"})
    SPY = SPY[["date", "open", "high", "low", "close", "volume"]]
    SPY = group_by_date(SPY)
    SPY = SPY.reset_index()
    SPY = create_rows_for_missing_dates(SPY)
    SPY = forward_fill_missing_data(SPY)
    SPY = SPY.reset_index().rename(columns={"index": "date"})
    SPY = SPY.set_index("date")
    SPY.to_parquet(os.path.join(CLEANED_DIR, "spx.parquet"))
    SPY.to_csv(os.path.join(CLEANED_DIR, "spx.csv"))

    # VIX
    VIX = pd.read_csv("data/raw/^VIX_1day_20231027_20251027.csv")
    VIX = VIX.rename(columns={"caldt": "date"})
    VIX = VIX[["date", "open", "high", "low", "close", "volume"]]
    VIX = create_rows_for_missing_dates(VIX)
    VIX = forward_fill_missing_data(VIX)
    VIX = VIX.reset_index().rename(columns={"index": "date"})
    VIX = VIX.set_index("date")
    VIX.to_parquet(os.path.join(CLEANED_DIR, "vix.parquet"))
    VIX.to_csv(os.path.join(CLEANED_DIR, "vix.csv"))

    # MOVE
    MOVE = pd.read_csv("data/raw/MOVE.csv")
    MOVE = MOVE.rename(columns={"Date": "date"})
    MOVE['date'] = pd.to_datetime(MOVE['date'], utc=True).dt.normalize().dt.tz_localize(None)
    MOVE.set_index('date', inplace=True)
    MOVE = MOVE[['Open', 'High', 'Low', 'Close', 'Volume']]
    MOVE = MOVE.reset_index()
    MOVE = create_rows_for_missing_dates(MOVE)
    MOVE = forward_fill_missing_data(MOVE)
    MOVE = MOVE.reset_index().rename(columns={"index": "date"})
    MOVE = MOVE.set_index("date")
    MOVE.to_parquet(os.path.join(CLEANED_DIR, "move.parquet"))
    MOVE.to_csv(os.path.join(CLEANED_DIR, "move.csv"))
    MOVE = MOVE.rename(columns=lambda x: x.lower() + '_MOVE')

    # TLT
    TLT = pd.read_csv("data/raw/TLT.csv")
    TLT = TLT.rename(columns={"Date": "date"})
    TLT['date'] = pd.to_datetime(TLT['date'], utc=True).dt.normalize().dt.tz_localize(None)
    TLT.set_index('date', inplace=True)
    TLT = TLT[['Open', 'High', 'Low', 'Close', 'Volume']]
    TLT = TLT.reset_index()
    TLT = create_rows_for_missing_dates(TLT)
    TLT = forward_fill_missing_data(TLT)
    TLT = TLT.reset_index().rename(columns={"index": "date"})
    TLT = TLT.set_index("date")
    TLT.to_parquet(os.path.join(CLEANED_DIR, "tlt.parquet"))
    TLT.to_csv(os.path.join(CLEANED_DIR, "tlt.csv"))
    TLT = TLT.rename(columns=lambda x: x.lower() + '_TLT')

    # Merge of all cleaned data
    df_final = SPY.join(VIX, lsuffix='_SPY', rsuffix='_VIX', how='outer')
    df_final = df_final.join(curve_10y_2y, how='outer')
    df_final = df_final.join(spread_baa_aaa, how='outer')
    df_final = df_final.join(MOVE, how='outer')
    df_final = df_final.join(TLT, how='outer')
    df_final = df_final.ffill()
    df_final = df_final.dropna()  # Remove any remaining NaN values, so that remains the last year
    df_final.to_csv("data/cleaned/market_macro_merged.csv", index=True, index_label="date")
    df_final.to_parquet("data/cleaned/market_macro_merged.parquet", index=True)
