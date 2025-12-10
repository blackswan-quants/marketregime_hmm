import logging
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller


LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ---------------------------------------------------------------------------
# Helpers to locate files in both old and new layouts
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]

DATA_PROCESSED = REPO_ROOT / "data" / "processed"


def _first_existing(paths: List[Path]) -> Path:
    """Finds the first existing path from a list of candidates.

    Args:
        paths (List[Path]): List of paths to check.

    Returns:
        Path: The first path that exists.

    Raises:
        FileNotFoundError: If none of the paths exist.
    """
    for p in paths:
        if p.exists():
            return p
    raise FileNotFoundError(f"None of these paths exist: {paths}")


def load_features_dataset() -> pd.DataFrame:
    """Loads the standardized feature dataset used for hypothesis checks.

    Tries to locate the features dataset in multiple potential locations.

    Returns:
        pd.DataFrame: The loaded features dataset, with date index if available.
    """
    path = _first_existing(
        [
            DATA_PROCESSED / "features_dataset.parquet",
            REPO_ROOT / "features_dataset.parquet",
        ]
    )
    LOGGER.info("Loading features dataset from %s", path)
    df = pd.read_parquet(path)
    if "date" in df.columns:
        df = df.set_index("date")
    return df


def load_distribution_stats() -> pd.DataFrame:
    """Loads distribution statistics (skewness, kurtosis, normality flag).

    Tries multiple paths to find the distribution statistics CSV.

    Returns:
        pd.DataFrame: DataFrame containing distribution statistics.
    """
    path = _first_existing(
        [
            DATA_PROCESSED / "distribution" / "distribution_stats.csv",
            REPO_ROOT / "distribution" / "distribution_stats.csv",
            REPO_ROOT / "distribution_stats.csv",
        ]
    )
    LOGGER.info("Loading distribution stats from %s", path)
    return pd.read_csv(path)


def load_hmm_input() -> Optional[pd.DataFrame]:
    """Loads HMM model input (factor scores), if available.

    Used mainly for sanity checks and potential future extensions.

    Returns:
        Optional[pd.DataFrame]: The HMM model input DataFrame if found, else None.
    """
    candidates = [
        DATA_PROCESSED / "hmm_input" / "hmm_model_input.parquet",
        DATA_PROCESSED / "hmm_input" / "hmm_input.parquet",
        REPO_ROOT / "hmm_input" / "hmm_model_input.parquet",
        REPO_ROOT / "hmm_model_input.parquet",
    ]
    try:
        path = _first_existing(candidates)
    except FileNotFoundError:
        LOGGER.warning("HMM input parquet not found; skipping factor-level checks.")
        return None

    LOGGER.info("Loading HMM model input from %s", path)
    df = pd.read_parquet(path)
    if "date" in df.columns:
        df = df.set_index("date")
    return df


def load_scree_tables() -> Dict[str, pd.DataFrame]:
    """Loads all scree CSVs (if present) into a dict keyed by filename stem.

    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping filename stems to DataFrames.
    """
    scree_dirs = [
        DATA_PROCESSED / "scree",
        REPO_ROOT / "scree",
        REPO_ROOT / "data" / "processed" / "scree",
    ]
    tables: Dict[str, pd.DataFrame] = {}

    for d in scree_dirs:
        if not d.exists():
            continue
        for csv_path in d.glob("*.csv"):
            LOGGER.info("Loading scree table from %s", csv_path)
            tables[csv_path.stem] = pd.read_csv(csv_path)

    if not tables:
        LOGGER.info("No scree CSVs found; skipping scree-based checks.")
    return tables


# ---------------------------------------------------------------------------
# Individual hypothesis checks
# ---------------------------------------------------------------------------


def adf_pvalue(series: pd.Series) -> float:
    """Return ADF p-value, handling short/NaN series defensively.

    Args:
        series (pd.Series): Time series to test.

    Returns:
        float: The p-value from the Augmented Dickey-Fuller test. Returns NaN if
            series is too short or test fails.
    """
    clean = series.dropna()
    if len(clean) < 5:
        return np.nan
    try:
        result = adfuller(clean, autolag="AIC")
        return float(result[1])
    except Exception:
        return np.nan


def check_returns_mean_and_stationarity(df: pd.DataFrame) -> pd.DataFrame:
    """Checks Hypothesis 1: Returns mean ≈ 0, stationarity confirmed.

    Operates on standardized return columns (R1_z, R2_z, M1_z, M2_z).

    Args:
        df (pd.DataFrame): DataFrame containing standardized features.

    Returns:
        pd.DataFrame: Summary of mean and stationarity checks.
    """
    candidate_cols = ["R1_z", "R2_z", "M1_z", "M2_z"]
    cols = [c for c in candidate_cols if c in df.columns]
    if not cols:
        LOGGER.warning("No standardized return columns found; skipping returns check.")
        return pd.DataFrame()

    rows = []
    for col in cols:
        s = df[col]
        mean = float(s.mean())
        std = float(s.std())
        p_adf = adf_pvalue(s)

        rows.append(
            {
                "feature": col,
                "mean": mean,
                "std": std,
                "adf_pvalue": p_adf,
                "mean_close_to_zero": abs(mean) < 0.1,
                "std_close_to_one": abs(std - 1.0) < 0.1,
                "stationary_adf": p_adf < 0.05 if not np.isnan(p_adf) else False,
            }
        )

    return pd.DataFrame(rows)


def check_volatility(df: pd.DataFrame) -> pd.DataFrame:
    """Checks Hypothesis 2: Volatility spikes align with known stress periods.

    Analyzes quantiles and max values of volatility proxies.

    Args:
        df (pd.DataFrame): DataFrame containing standardized features.

    Returns:
        pd.DataFrame: Summary of volatility distribution statistics.
    """
    candidate_cols = [
        "vix_close_z",
        "V1_z",
        "V2_z",
        "V3_hl10_z",
        "V3_hl20_z",
        "M_LV_MOVE_level_z",
    ]
    cols = [c for c in candidate_cols if c in df.columns]
    if not cols:
        LOGGER.warning("No volatility proxy columns found; skipping volatility check.")
        return pd.DataFrame()

    quantiles = [0.5, 0.9, 0.95, 0.99]

    rows = []
    for col in cols:
        s = df[col].dropna()
        row = {
            "feature": col,
            "mean": float(s.mean()),
            "std": float(s.std()),
            "max": float(s.max()),
        }
        for q in quantiles:
            row[f"q{int(q*100)}"] = float(s.quantile(q))
        rows.append(row)

    return pd.DataFrame(rows)


def check_curve_and_spreads(df: pd.DataFrame) -> pd.DataFrame:
    """Checks Hypothesis 3: Credit spreads / curve slope signals.

    Summarizes curve inversion frequency and credit spread tail behavior.

    Args:
        df (pd.DataFrame): DataFrame containing standardized features.

    Returns:
        pd.DataFrame: Summary of curve and spread metrics.
    """
    curve_col = "curve_10y_2y_z"
    spread_col = "credit_spread_baa_aaa_z"

    rows = []

    if curve_col in df.columns:
        curve = df[curve_col].dropna()
        inv_mask = curve < 0
        rows.append(
            {
                "feature": curve_col,
                "metric": "curve_inversion_fraction",
                "value": float(inv_mask.mean()),
            }
        )
        rows.append(
            {
                "feature": curve_col,
                "metric": "curve_min_value",
                "value": float(curve.min()),
            }
        )

    if spread_col in df.columns:
        spread = df[spread_col].dropna()
        for q in (0.9, 0.95, 0.99):
            rows.append(
                {
                    "feature": spread_col,
                    "metric": f"spread_q{int(q*100)}",
                    "value": float(spread.quantile(q)),
                }
            )
        rows.append(
            {
                "feature": spread_col,
                "metric": "spread_mean",
                "value": float(spread.mean()),
            }
        )

    if not rows:
        LOGGER.warning(
            "No curve/spread columns found; skipping curve/spread checks."
        )

    return pd.DataFrame(rows)


def check_cross_asset_correlations(df: pd.DataFrame) -> pd.DataFrame:
    """Checks Hypothesis 4: Cross-asset correlations and tail behavior.

    Summarizes distribution of correlation and beta features.

    Args:
        df (pd.DataFrame): DataFrame containing standardized features.

    Returns:
        pd.DataFrame: Summary of correlation/beta statistics.
    """
    corr_like = [
        c
        for c in df.columns
        if ("corr" in c.lower()) or ("beta" in c.lower())
    ]
    if not corr_like:
        LOGGER.warning(
            "No correlation/beta-like columns found; skipping cross-asset checks."
        )
        return pd.DataFrame()

    rows = []
    for col in corr_like:
        s = df[col].dropna()
        rows.append(
            {
                "feature": col,
                "mean": float(s.mean()),
                "std": float(s.std()),
                "min": float(s.min()),
                "max": float(s.max()),
                "q5": float(s.quantile(0.05)),
                "q95": float(s.quantile(0.95)),
            }
        )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def verify_financial_hypotheses() -> None:
    """Orchestrates the verification of financial hypotheses.

    1. Loads standardized features and supporting stats.
    2. Runs quantitative checks for each financial hypothesis.
    3. Saves CSV summaries to data/reports for inclusion in the final report.
    """
    df_features = load_features_dataset()
    dist_stats = load_distribution_stats()
    _ = load_hmm_input()
    _ = load_scree_tables()  # currently unused, but loaded for completeness

    # Make sure we only work on standardized columns by default.
    # This is consistent with the HMM assumptions. :contentReference[oaicite:1]{index=1}
    z_cols = [c for c in df_features.columns if c.endswith("_z")]
    df_z = df_features[z_cols].copy()

    LOGGER.info("Running returns mean/stationarity checks.")
    returns_df = check_returns_mean_and_stationarity(df_z)

    LOGGER.info("Running volatility distribution checks.")
    vol_df = check_volatility(df_z)

    LOGGER.info("Running curve & credit spread checks.")
    curve_spread_df = check_curve_and_spreads(df_z)

    LOGGER.info("Running cross-asset correlation/beta checks.")
    cross_asset_df = check_cross_asset_correlations(df_z)

    # Join everything into a single multi-section dictionary of DataFrames.
    reports_dir = REPO_ROOT / "data" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Save individual CSVs
    if not returns_df.empty:
        returns_df.to_csv(
            reports_dir / "financial_hypothesis_returns.csv", index=False
        )
    if not vol_df.empty:
        vol_df.to_csv(
            reports_dir / "financial_hypothesis_volatility.csv", index=False
        )
    if not curve_spread_df.empty:
        curve_spread_df.to_csv(
            reports_dir / "financial_hypothesis_curve_spreads.csv", index=False
        )
    if not cross_asset_df.empty:
        cross_asset_df.to_csv(
            reports_dir / "financial_hypothesis_cross_asset.csv", index=False
        )

    # Also save the distribution stats alongside, since they are referenced
    # when discussing Gaussianity of standardized features.
    dist_stats.to_csv(
        reports_dir / "distribution_stats_copy_for_report.csv", index=False
    )

    LOGGER.info("Financial hypotheses verification completed.")
    LOGGER.info("Reports written to %s", reports_dir)


if __name__ == "__main__":
    verify_financial_hypotheses()
