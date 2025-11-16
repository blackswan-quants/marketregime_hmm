import logging
import os

import numpy as np
import pandas as pd

CLEANED_DIR = "data/cleaned"
PROCESSED_DIR = "data/processed"
TRADING_DAYS = 252

logger = logging.getLogger(__name__)


def load_spx_vix() -> pd.DataFrame:
    """Loads SPX and VIX data sets and aligns them on the date index.

    Returns:
        DataFrame with columns:
            - spx_close
            - vix_close
    """
    spx_path = os.path.join(CLEANED_DIR, "spx.parquet")
    vix_path = os.path.join(CLEANED_DIR, "vix.parquet")

    spx = pd.read_parquet(spx_path)
    vix = pd.read_parquet(vix_path)

    spx = spx.sort_index()
    vix = vix.sort_index()

    spx = spx[["close"]].rename(columns={"close": "spx_close"})
    vix = vix[["close"]].rename(columns={"close": "vix_close"})

    df = spx.join(vix, how="inner").sort_index()
    return df


# Helper funtions


def log_return(prices: pd.Series, window: int) -> pd.Series:
    """Log-return over window days: ln(P_t) - ln(P_{t-window})."""
    return np.log(prices).diff(window)


def realized_vol(returns_1d: pd.Series, window: int) -> pd.Series:
    """Realized volatility over a rolling window, annualized."""
    rolling_var = returns_1d.pow(2).rolling(window=window).mean()
    return np.sqrt(TRADING_DAYS * rolling_var)


def ewma_vol(returns_1d: pd.Series, halflife: int) -> pd.Series:
    """EWMA volatility estimate, annualized."""
    ewma_var = returns_1d.pow(2).ewm(halflife=halflife, adjust=False).mean()
    return np.sqrt(TRADING_DAYS * ewma_var)


def compute_max_drawdown_window(window_prices: pd.Series) -> float:
    """Compute max drawdown inside a window of prices.
    Max drawdown = worst drop from a previous peak in this window.
    Args:
        window_prices: Prices in the current rolling window.

    Returns:
        The most negative drawdown value in the window.
    """
    running_peak = window_prices.cummax()
    drawdowns = window_prices / running_peak - 1.0
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
def build_market_features(df: pd.DataFrame) -> pd.DataFrame:
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
    df_["V1"] = realized_vol(df_["R1"], window=14)
    df_["V2"] = realized_vol(df_["R1"], window=42)

    # V3
    df_["V3_hl10"] = ewma_vol(df_["R1"], halflife=10)
    df_["V3_hl20"] = ewma_vol(df_["R1"], halflife=20)

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

    df_inputs = load_spx_vix()
    df_features = build_market_features(df_inputs)
    save_market_features(df_features)
    print(df_features.tail())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
    )
    main()
