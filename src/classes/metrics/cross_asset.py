"""Cross-asset and rates volatility feature construction.

Person 5 — Cross-Asset & Rates Volatility.

Inputs (from data/cleaned/):
  - spx.parquet   (SPX index prices)
  - tlt.parquet   (TLT ETF prices)
  - move.parquet  (MOVE index levels)

Outputs (to data/processed/):
  - cross_asset_features.parquet

Features:
  - X1: Corr(SPX, TLT) 42d / EWMA corr (halflife 10, 20)
  - X2: Beta SPX <- TLT (42d, 63d, EWMA OLS halflife 10, 20)
  - X3: Momentum Diff 42d (SPX – TLT)
  - M_LV: MOVE level
  - M_CH: ΔMOVE 14d / 21d
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths (robust to current working directory)
# ---------------------------------------------------------------------------

# This file lives in: src/classes/metrics/cross-asset.py
# So the project root is 3 levels above: metrics -> classes -> src -> root.
PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

DEFAULT_INPUT_DIR = DATA_CLEANED_DIR
DEFAULT_OUTPUT_PATH = DATA_PROCESSED_DIR / "cross_asset_features.parquet"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def load_series(
    input_dir: Union[str, Path],
    filename: str,
    column_name: str,
    date_col: str = "date",
    value_col: Optional[str] = None,
) -> pd.DataFrame:
    """Loads a single time series from parquet and standardizes its structure.

    The function:
      - reads a parquet file,
      - ensures there is a datetime index (using `date_col` if present,
        otherwise using the existing index),
      - sorts by date,
      - renames the chosen value column to `column_name`.

    If `value_col` is None, the function tries to infer it by looking for
    commonly used names such as 'close' or 'Close'.

    Args:
      input_dir: Directory containing the input file.
      filename: Name of the parquet file (e.g. 'spx.parquet').
      column_name: Name to assign to the value column (e.g. 'SPX').
      date_col: Name of the date column in the parquet file, if present.
      value_col: Name of the value column in the parquet file. If None,
        the function tries to pick a reasonable price column.

    Returns:
      A DataFrame indexed by date with a single column named `column_name`.

    Raises:
      ValueError: If `value_col` is None and no obvious price column is found.
    """
    input_dir = Path(input_dir)
    path = input_dir / filename

    df = pd.read_parquet(path)

    # --- Handle the date / index logic ---
    if date_col in df.columns:
        # Explicit 'date' column present.
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col).set_index(date_col)
    elif "index" in df.columns:
        # Handle default reset_index() column from data_manager.py
        df["index"] = pd.to_datetime(df["index"])
        df = df.sort_values("index").set_index("index")
    else:
        # No explicit 'date' column: assume index carries the dates.
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()

    # --- Infer value column if not provided ---
    if value_col is None:
        candidates = [
            "close",
            "Close",
            "adj_close",
            "Adj Close",
            "Open",
            "open",
            "close_SPY",
            "close_TLT",
            "close_MOVE",
        ]
        found = [c for c in candidates if c in df.columns]
        if not found:
            raise ValueError(f"Could not infer value column for {filename}. " f"Available columns: {list(df.columns)}")
        value_col = found[0]

    df = df[[value_col]].rename(columns={value_col: column_name})
    return df


# ---------------------------------------------------------------------------
# Cross-asset feature construction helpers
# ---------------------------------------------------------------------------


def compute_log_returns(df: pd.DataFrame, price_col: str) -> pd.Series:
    """Computes log returns from a price series.

    Args:
      df: DataFrame containing the price series.
      price_col: Name of the price column.

    Returns:
      A Series of log returns: ln(P_t) - ln(P_{t-1}).
    """
    return np.log(df[price_col]).diff()


def add_x1_correlation_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adds X1 correlation features between SPX and TLT returns.

    X1 features:
      - X1_corr_42: rolling 42-day Pearson correlation.
      - X1_corr_ewm_hl10: EWMA correlation with halflife 10.
      - X1_corr_ewm_hl20: EWMA correlation with halflife 20.

    Args:
      df: DataFrame with columns 'SPX_ret' and 'TLT_ret'.

    Returns:
      DataFrame with additional X1 features.
    """
    df = df.copy()
    window_corr = 42

    df["X1_corr_42"] = df["SPX_ret"].rolling(window_corr).corr(df["TLT_ret"])

    for halflife in (10, 20):
        df[f"X1_corr_ewm_hl{halflife}"] = df["SPX_ret"].ewm(halflife=halflife, adjust=False).corr(df["TLT_ret"])

    return df


def add_x2_beta_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adds X2 beta features: SPX <- TLT (equity vs duration moves).

    X2 features use the OLS relation:
      r_SPX = alpha + beta * r_TLT + epsilon

    Implemented as:
      beta = Cov(SPX_ret, TLT_ret) / Var(TLT_ret)

    Features:
      - X2_beta_42: rolling beta with 42-day window.
      - X2_beta_63: rolling beta with 63-day window.
      - X2_beta_ewm_hl10: EWMA beta (halflife 10).
      - X2_beta_ewm_hl20: EWMA beta (halflife 20).

    Args:
      df: DataFrame with columns 'SPX_ret' and 'TLT_ret'.

    Returns:
      DataFrame with additional X2 features.
    """
    df = df.copy()

    # Rolling betas.
    for window in (42, 63):
        cov = df["SPX_ret"].rolling(window).cov(df["TLT_ret"])
        var = df["TLT_ret"].rolling(window).var()
        df[f"X2_beta_{window}"] = np.where(var != 0, cov / var, np.nan)

    # EWMA betas.
    for halflife in (10, 20):
        cov_ewm = df["SPX_ret"].ewm(halflife=halflife, adjust=False).cov(df["TLT_ret"])
        var_ewm = df["TLT_ret"].ewm(halflife=halflife, adjust=False).var()
        df[f"X2_beta_ewm_hl{halflife}"] = np.where(var_ewm != 0, cov_ewm / var_ewm, np.nan)

    return df


def add_x3_momentum_diff_feature(df: pd.DataFrame) -> pd.DataFrame:
    """Adds X3 momentum difference feature between SPX and TLT.

    X3 is defined as:
      mom_SPX_42 - mom_TLT_42,
    where:
      mom_42 = ln(P_t) - ln(P_{t-42})

    Args:
      df: DataFrame with price columns 'SPX' and 'TLT'.

    Returns:
      DataFrame with additional X3 feature 'X3_mom_diff_42'.
    """
    df = df.copy()
    window = 42

    df["SPX_mom_42"] = np.log(df["SPX"]).diff(window)
    df["TLT_mom_42"] = np.log(df["TLT"]).diff(window)
    df["X3_mom_diff_42"] = df["SPX_mom_42"] - df["TLT_mom_42"]

    return df


def add_move_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adds MOVE-related features: level and 14/21-day changes.

    Features:
      - M_LV_MOVE_level: raw MOVE level (rates volatility proxy).
      - M_CH_MOVE_d14: ΔMOVE over 14 days.
      - M_CH_MOVE_d21: ΔMOVE over 21 days.

    Args:
      df: DataFrame with column 'MOVE' (MOVE index level).

    Returns:
      DataFrame with additional MOVE-related features.
    """
    df = df.copy()

    # Level.
    df["M_LV_MOVE_level"] = df["MOVE"]

    # Changes over 14d and 21d.
    for window in (14, 21):
        df[f"M_CH_MOVE_d{window}"] = df["MOVE"].diff(window)

    return df


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------


def build_cross_asset_features(
    input_dir: Union[str, Path] = DEFAULT_INPUT_DIR,
    spx_filename: str = "spx.parquet",
    tlt_filename: str = "tlt.parquet",
    move_filename: str = "move.parquet",
) -> pd.DataFrame:
    """Builds cross-asset and rates volatility features.

    This function:
      - loads SPX, TLT, MOVE series from parquet,
      - aligns them on a common calendar (inner join),
      - computes SPX/TLT daily log returns,
      - constructs X1, X2, X3, M_LV, M_CH features.

    Args:
      input_dir: Directory containing the cleaned input files.
      spx_filename: Filename for SPX prices in `input_dir`.
      tlt_filename: Filename for TLT prices in `input_dir`.
      move_filename: Filename for MOVE index in `input_dir`.

    Returns:
      A DataFrame with cross-asset features (raw values only).
    """
    # Load base series.
    spx = load_series(
        input_dir=input_dir,
        filename=spx_filename,
        column_name="SPX",
        date_col="date",
    )

    tlt = load_series(
        input_dir=input_dir,
        filename=tlt_filename,
        column_name="TLT",
        date_col="date",
    )

    move = load_series(
        input_dir=input_dir,
        filename=move_filename,
        column_name="MOVE",
        date_col="date",
    )

    # Align on common calendar (SPX ∩ TLT ∩ MOVE).
    df = spx.join(tlt, how="inner").join(move, how="inner")

    # Compute daily log returns for SPX and TLT.
    df["SPX_ret"] = compute_log_returns(df, "SPX")
    df["TLT_ret"] = compute_log_returns(df, "TLT")

    # Add cross-asset features.
    df = add_x1_correlation_features(df)
    df = add_x2_beta_features(df)
    df = add_x3_momentum_diff_feature(df)
    df = add_move_features(df)

    # Feature columns to keep.
    feature_cols = [
        "X1_corr_42",
        "X1_corr_ewm_hl10",
        "X1_corr_ewm_hl20",
        "X2_beta_42",
        "X2_beta_63",
        "X2_beta_ewm_hl10",
        "X2_beta_ewm_hl20",
        "X3_mom_diff_42",
        "M_LV_MOVE_level",
        "M_CH_MOVE_d14",
        "M_CH_MOVE_d21",
    ]

    out_df = df[feature_cols].copy()
    out_df.index.name = "date"
    return out_df


def save_cross_asset_features(
    output_path: Union[str, Path] = DEFAULT_OUTPUT_PATH,
    **build_kwargs,
) -> None:
    """Builds and saves cross-asset features to a parquet file.

    Args:
      output_path: Path where the parquet file will be written.
      **build_kwargs: Additional keyword arguments passed to
        `build_cross_asset_features`, e.g.:
          - input_dir="data/cleaned"
          - spx_filename="spx.parquet"
          - tlt_filename="tlt.parquet"
          - move_filename="move.parquet"
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    features = build_cross_asset_features(**build_kwargs)
    features.to_parquet(output_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    save_cross_asset_features()
    logger.info("Saved cross-asset features to: %s", DEFAULT_OUTPUT_PATH)
