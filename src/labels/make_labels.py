"""
Task P6: Label Generation for Market Regimes

This module generates preliminary regime labels based on feature engineering output.
It implements two labeling schemes:
  1. Volatility Buckets (3 states: Low, Mid, High)
  2. Risk Regimes (3 states: Risk-On, Risk-Off, Neutral)

Output: labels_prelim.csv with columns [vol_bucket, risk_regime]

NOTE: Column names must match the output of P2 (feature engineering).
Configuration section below allows for easy remapping if names change.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION: Column Name Mapping
# ============================================================================
# Update these when P2 completes and actual column names are known.
# Current placeholder names based on feature engineering spec.

FEATURE_COLUMNS = {
    # Volatility feature (for tertiles)
    "volatility": "V1",  # EWMA volatility
    # Momentum features
    "momentum_short": "R1",  # 10-day momentum
    "momentum_long": "R2",  # 42-day momentum
    # Risk indicators
    "vix_ewma_ratio": "V2",  # VIX / EWMA Vol ratio
    "yc_slope": "M1",  # Yield curve slope (10y - 2y)
    "credit_spread_change": "M2",  # Credit spread change
    # Returns (for validation/reporting)
    "returns_1d": "D1",  # 1-day returns
}

INPUT_PARQUET = Path("data/processed/features_dataset.parquet")
OUTPUT_CSV = Path("data/processed/labels_prelim.csv")


# ============================================================================
# VOLATILITY BUCKET LABELING
# ============================================================================


def compute_volatility_buckets(df: pd.DataFrame, vol_col: str) -> pd.Series:
    """
    Create 3-state volatility labels based on tertiles.

    Args:
        df: DataFrame with volatility column
        vol_col: Name of volatility column

    Returns:
        Series with labels: 'Low_Vol', 'Mid_Vol', 'High_Vol'
    """
    # Compute 33rd and 66th percentiles
    p33 = df[vol_col].quantile(0.33)
    p66 = df[vol_col].quantile(0.66)

    labels = pd.Series(index=df.index, dtype="object")
    labels[df[vol_col] <= p33] = "Low_Vol"
    labels[(df[vol_col] > p33) & (df[vol_col] <= p66)] = "Mid_Vol"
    labels[df[vol_col] > p66] = "High_Vol"

    return labels


# ============================================================================
# RISK REGIME LABELING (Heuristic Rules)
# ============================================================================


def compute_risk_regimes(df: pd.DataFrame, config: dict) -> pd.Series:
    """
    Create 3-state risk regime labels based on heuristic rules.

    Risk-On Condition (AND logic):
        (momentum_short > 0) & (vix_ewma_ratio < 1.1)

    Risk-Off Condition (OR logic):
        (momentum_long < 0) | (yc_slope < 0) | (credit_spread_change > 0)

    Default: Neutral (if neither condition strictly met)

    Args:
        df: DataFrame with feature columns
        config: Dict mapping feature names to actual column names

    Returns:
        Series with labels: 'Risk-On', 'Risk-Off', 'Neutral'
    """
    # Extract column names from config
    mom_short = config.get("momentum_short")
    mom_long = config.get("momentum_long")
    vix_ratio = config.get("vix_ewma_ratio")
    yc_slope = config.get("yc_slope")
    credit_change = config.get("credit_spread_change")

    # Validate required columns exist
    required_cols = [mom_short, mom_long, vix_ratio, yc_slope, credit_change]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Define conditions
    risk_on_condition = (df[mom_short] > 0) & (df[vix_ratio] < 1.1)
    risk_off_condition = (df[mom_long] < 0) | (df[yc_slope] < 0) | (df[credit_change] > 0)

    # Assign labels (Risk-Off has priority)
    labels = pd.Series(index=df.index, dtype="object")
    labels[:] = "Neutral"  # Default
    labels[risk_on_condition & ~risk_off_condition] = "Risk-On"
    labels[risk_off_condition] = "Risk-Off"

    return labels


# ============================================================================
# MAIN PIPELINE
# ============================================================================


def make_labels(
    input_path: Optional[Path] = None, output_path: Optional[Path] = None, feature_config: Optional[dict] = None
) -> pd.DataFrame:
    """
    Main pipeline: Load features, compute labels, save output.

    Args:
        input_path: Path to input parquet file. Defaults to INPUT_PARQUET.
        output_path: Path to output CSV file. Defaults to OUTPUT_CSV.
        feature_config: Column name mapping. Defaults to FEATURE_COLUMNS.

    Returns:
        DataFrame with columns: [vol_bucket, risk_regime]
    """
    input_path = input_path or INPUT_PARQUET
    output_path = output_path or OUTPUT_CSV
    feature_config = feature_config or FEATURE_COLUMNS

    # Load data
    logger.info("Loading features from %s", input_path)
    df = pd.read_parquet(input_path)

    # Set date column as index if it exists
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

    logger.info("Loaded %s rows, %s columns", df.shape[0], df.shape[1])
    logger.info("Date range: %s to %s", df.index.min(), df.index.max())

    # TODO: Update FEATURE_COLUMNS dict when P2 completes
    # Check if expected columns exist; if not, attempt auto-detection
    vol_col = feature_config.get("volatility")
    if vol_col not in df.columns:
        logger.warning("Column '%s' not found. Available: %s", vol_col, df.columns.tolist())
        # Placeholder: auto-detect or ask user
        raise ValueError(f"Column '{vol_col}' not found in dataset.")

    # Compute volatility buckets
    logger.info("Computing volatility buckets")
    vol_bucket = compute_volatility_buckets(df, vol_col)

    # Compute risk regimes
    logger.info("Computing risk regimes")
    risk_regime = compute_risk_regimes(df, feature_config)

    # Create output DataFrame
    labels_df = pd.DataFrame(
        {
            "vol_bucket": vol_bucket,
            "risk_regime": risk_regime,
        },
        index=df.index,
    )

    # Summary statistics
    logger.info("%s", "=" * 60)
    logger.info("LABEL SUMMARY STATISTICS")
    logger.info("%s", "=" * 60)
    logger.info("Volatility Bucket Distribution:\n%s", labels_df["vol_bucket"].value_counts().to_string())
    logger.info("Risk Regime Distribution:\n%s", labels_df["risk_regime"].value_counts().to_string())
    logger.info(
        "Cross-tabulation:\n%s",
        pd.crosstab(labels_df["vol_bucket"], labels_df["risk_regime"]).to_string(),
    )

    # Save to CSV
    logger.info("Saving labels to %s", output_path)
    labels_df.to_csv(output_path)
    logger.info("Done")

    return labels_df


if __name__ == "__main__":
    labels_df = make_labels()
