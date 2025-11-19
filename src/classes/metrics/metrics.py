import logging
import os

import numpy as np
import pandas as pd

CLEANED_DIR = "data/cleaned"
PROCESSED_DIR = "data/processed"

logger = logging.getLogger(__name__)


def load_spx_vix() -> tuple[pd.DataFrame, int]:
    """Loads SPX and VIX and aligns them to the master calendar (market_macro_merged.parquet).

    Returns:
    DataFrame with columns:
        - spx_close
        - vix_close
    trading_days: number of trading days per year from the calendar
    """

    spx_path = os.path.join(CLEANED_DIR, "spx.parquet")
    vix_path = os.path.join(CLEANED_DIR, "vix.parquet")
    calendar_path = os.path.join(CLEANED_DIR, "market_macro_merged.parquet")

    macro = pd.read_parquet(calendar_path)
    spx = pd.read_parquet(spx_path)
    vix = pd.read_parquet(vix_path)

    if "date" in macro.columns:
        macro["date"] = pd.to_datetime(macro["date"])
        macro = macro.set_index("date")
    else:
        macro.index = pd.to_datetime(macro.index)

    calendar = macro.index

    if "date" in spx.columns:
        spx["date"] = pd.to_datetime(spx["date"])
        spx = spx.set_index("date")
    else:
        spx.index = pd.to_datetime(spx.index)

    if "date" in vix.columns:
        vix["date"] = pd.to_datetime(vix["date"])
        vix = vix.set_index("date")
    else:
        vix.index = pd.to_datetime(vix.index)

    # Reindexing to calendar and forward filling of missing values
    spx = spx.reindex(calendar).ffill()
    vix = vix.reindex(calendar).ffill()

    # Renaming the columns
    spx = spx[["close"]].rename(columns={"close": "spx_close"})
    vix = vix[["close"]].rename(columns={"close": "vix_close"})

    df = pd.DataFrame(index=calendar)
    df["spx_close"] = spx["spx_close"]
    df["vix_close"] = vix["vix_close"]

    # Counts how many dates appear in each year, then take the average
    unique_dates = pd.DatetimeIndex(df.index.unique())
    counts_per_year = unique_dates.year.value_counts().sort_index()
    trading_days = int(counts_per_year.mean())
    logger.info("Trading days per year: %d", trading_days)
    return df, trading_days


# Helper funtions


def log_return(prices: pd.Series, window: int) -> pd.Series:
    """Log-return over window days: ln(Pt) - ln(P{t_window}).
    Args:
        prices (pd.Series): Price series indexed by date.
        window (int): Rolling window length in days.

    Returns:
        pd.Series: Log-return series.


    """
    return np.log(prices).diff(window)


def realized_vol(returns_1d: pd.Series, window: int, trading_days: int) -> pd.Series:
    """Realized volatility over a rolling window, annualized.

    Args:
        returns_1d (pd.Series): Daily log returns indexed by date.
        window (int): Rolling window length in days.
        trading_days: number of trading days per year from the calendar

    Returns:
        pd.Series: Annualized realized volatility series.


    """
    rolling_var = returns_1d.pow(2).rolling(window=window).mean()
    return np.sqrt(trading_days * rolling_var)


def ewma_vol(returns_1d: pd.Series, halflife: int, trading_days: int) -> pd.Series:
    """EWMA volatility estimate, annualized.

    Args:
        returns_1d (pd.Series): Daily log returns indexed by date.
        halflife (int): EWMA decay halflife in days.
        trading_days: number of trading days per year from the calendar

    Returns:
        pd.Series: Annualized EWMA volatility series.

    """
    ewma_var = returns_1d.pow(2).ewm(halflife=halflife, adjust=False).mean()
    return np.sqrt(trading_days * ewma_var)


def compute_max_drawdown_window(window_prices: pd.Series) -> float:
    """Computes max drawdown inside a window of prices.
    Max drawdown = worst drop from a previous peak in this window.
    Args:
        window_prices: Prices in the current rolling window.

    Returns:
        The most negative drawdown value in the window.
    """
    running_peak = window_prices.cummax()
    drawdowns = (window_prices - running_peak) / running_peak
    return drawdowns.min()  # lowest value


def max_drawdown_rolling(prices: pd.Series, window: int = 42) -> pd.Series:
    """Rolling max drawdown (last 42 days).
    Each day we look back `window` days and compute the worst drawdown
    in that window.

    Args:
        prices: Daily price series.
        window: Number of days in the rolling lookback window.

    Returns:
        A series of rolling max drawdowns.
    """
    return prices.rolling(window=window, min_periods=window).apply(compute_max_drawdown_window, raw=False)


# All metrics
def build_market_features(df: pd.DataFrame, trading_days: int) -> pd.DataFrame:
    """Builds all market features (R1, R2, M1, M2, V1, V2, V3, S1, D1).

    Args:
        df: A DataFrame containing the columns:
            - spx_close
            - vix_close
    Returns:
        pd.DataFrame: A DataFrame containing the original input columns plus
        calculated metrics
    """

    logger.info("Building market features from input DataFrame with %d rows", len(df))
    df_ = df.copy()

    # R1, R2
    df_["R1"] = log_return(df_["spx_close"], window=1)
    df_["R2"] = log_return(df_["spx_close"], window=5)

    # M1, M2
    df_["M1"] = log_return(df_["spx_close"], window=10)
    df_["M2"] = log_return(df_["spx_close"], window=42)

    # V1, V2
    df_["V1"] = realized_vol(df_["R1"], 14, trading_days)
    df_["V2"] = realized_vol(df_["R1"], 42, trading_days)

    # V3
    df_["V3_hl10"] = ewma_vol(df_["R1"], 10, trading_days)
    df_["V3_hl20"] = ewma_vol(df_["R1"], 20, trading_days)

    # S1
    vix_decimal = df_["vix_close"] / 100.0
    df_["S1_hl10"] = vix_decimal / df_["V3_hl10"]
    df_["S1_hl20"] = vix_decimal / df_["V3_hl20"]

    # D1
    df_["D1"] = max_drawdown_rolling(df_["spx_close"], window=42)

    # Drop NaN rows caused by rolling windows
    df_ = df_.dropna()
    return df_


def save_market_features(df: pd.DataFrame) -> str:
    """Saves market features to the given directory.

    Args:
        df: DataFrame to save.

    Returns:
        Path of the written parquet file.
    """
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    output_path = os.path.join(PROCESSED_DIR, "market_features.parquet")
    df.to_parquet(output_path)
    logger.info("Saved market features to %s", output_path)
    return output_path


def main():

    df_inputs, trading_days = load_spx_vix()
    df_features = build_market_features(df_inputs, trading_days)
    save_market_features(df_features)
    logger.info("\n%s", df_features.tail())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
    )
    main()
