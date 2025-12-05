import logging
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.stattools import adfuller, kpss

# Configure logging for module-level usage if imported, though usually configured at entry point
logger = logging.getLogger(__name__)


def zscore_check_and_apply(
    df: pd.DataFrame,
    raw_cols: list[str],
    tol_mean: float = 1e-1,
    tol_std: float = 1e-1,
) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    """Checks if raw_cols in df are already z-scored. If not, applies StandardScaler.

    Args:
        df (pd.DataFrame): Input DataFrame containing the data.
        raw_cols (list[str]): List of column names to check/normalize.
        tol_mean (float): Tolerance for mean check (default 0.1).
        tol_std (float): Tolerance for std check (default 0.1).

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, bool]:
            - z_df_raw (pd.DataFrame): DataFrame with z-scored values.
            - norm_stats (pd.DataFrame): DataFrame with mean and std of original data.
            - already_standardized (bool): True if data was already standardized.
    """
    if not raw_cols:
        return (
            pd.DataFrame(index=df.index),
            pd.DataFrame(columns=["feature", "mean", "std"]),
            True,
        )

    sub_df = df[raw_cols]
    means = sub_df.mean()
    stds = sub_df.std()

    # Check if already standardized
    is_mean_ok = (means.abs() < tol_mean).all()
    is_std_ok = ((stds - 1).abs() < tol_std).all()
    already_standardized = is_mean_ok and is_std_ok

    norm_stats = pd.DataFrame(
        {"feature": raw_cols, "mean": means.values, "std": stds.values}
    )

    if already_standardized:
        z_df_raw = sub_df.copy()
    else:
        scaler = StandardScaler()
        z_values = scaler.fit_transform(sub_df)
        z_df_raw = pd.DataFrame(z_values, index=sub_df.index, columns=raw_cols)

    return z_df_raw, norm_stats, already_standardized


def run_adf(series: pd.Series) -> dict[str, float | int | bool]:
    """Runs Augmented Dickey-Fuller test on a series.

    Args:
        series (pd.Series): Time series data to test.

    Returns:
        dict[str, float | int | bool]: Dictionary containing:
            - "stat" (float): Test statistic.
            - "pvalue" (float): p-value.
            - "nobs" (int): Number of observations used.
            - "valid" (bool): Whether the test was valid.
    """
    clean_series = series.dropna()
    if len(clean_series) < 2:  # Basic check for sufficient data
        return {"stat": np.nan, "pvalue": np.nan, "nobs": len(series), "valid": False}

    try:
        result = adfuller(clean_series, autolag="AIC")
        return {
            "stat": result[0],
            "pvalue": result[1],
            "nobs": result[3],  # number of observations used
            "valid": True,
        }
    except Exception:
        return {"stat": np.nan, "pvalue": np.nan, "nobs": len(series), "valid": False}


def run_kpss(series: pd.Series, regression: str = "c") -> dict[str, float | int | bool]:
    """Runs KPSS test on a series.

    Args:
        series (pd.Series): Time series data to test.
        regression (str): Regression option for KPSS ("c" for constant, "ct" for trend).

    Returns:
        dict[str, float | int | bool]: Dictionary containing:
            - "stat" (float): Test statistic.
            - "pvalue" (float): p-value.
            - "nobs" (int): Number of observations used.
            - "valid" (bool): Whether the test was valid.
    """
    clean_series = series.dropna()
    if len(clean_series) < 2:
        return {"stat": np.nan, "pvalue": np.nan, "nobs": len(series), "valid": False}

    try:
        # Suppress warnings from statsmodels if needed, but usually fine to leave
        with np.errstate(
            all="ignore"
        ):  # Catch warnings as errors if strict, but here just run
            # Note: kpss can issue warnings for interpolation
            stat, p_value, lags, crit = kpss(
                clean_series, regression=regression, nlags="auto"
            )
        return {
            "stat": stat,
            "pvalue": p_value,
            "nobs": len(clean_series),
            "valid": True,
        }
    except Exception:
        return {"stat": np.nan, "pvalue": np.nan, "nobs": len(series), "valid": False}


def stationarity_report(
    df: pd.DataFrame, zscore_cols: list[str], alpha: float = 0.05
) -> pd.DataFrame:
    """Runs ADF and KPSS tests on zscore_cols and produces a report.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        zscore_cols (list[str]): List of column names to test.
        alpha (float): Significance level for hypothesis testing (default 0.05).

    Returns:
        pd.DataFrame: DataFrame containing test results and verdicts for each feature.
    """
    results = []

    for col in zscore_cols:
        series = df[col]
        adf_res = run_adf(series)
        kpss_res = run_kpss(series)

        adf_p = adf_res["pvalue"]
        kpss_p = kpss_res["pvalue"]

        # Determine stationarity based on p-values
        # ADF: H0 = unit root (non-stationary). p < alpha => stationary
        # KPSS: H0 = stationary. p > alpha => stationary

        is_adf_stat = False
        if adf_res["valid"] and not np.isnan(adf_p):  # type: ignore
            is_adf_stat = adf_p < alpha  # type: ignore

        is_kpss_stat = False
        if kpss_res["valid"] and not np.isnan(kpss_p):  # type: ignore
            is_kpss_stat = kpss_p > alpha  # type: ignore

        verdict = "invalid"
        if not adf_res["valid"] or not kpss_res["valid"]:
            verdict = "invalid"
        elif is_adf_stat and is_kpss_stat:
            verdict = "stationary"
        elif not is_adf_stat and is_kpss_stat:
            verdict = "likely_non_stationary"
        elif is_adf_stat and not is_kpss_stat:
            verdict = "weak_stationary"
        else:  # not is_adf_stat and not is_kpss_stat
            verdict = "conflict"

        results.append(
            {
                "feature": col,
                "adf_stat": adf_res["stat"],
                "adf_pvalue": adf_p,
                "kpss_stat": kpss_res["stat"],
                "kpss_pvalue": kpss_p,
                "adf_stationary": is_adf_stat,
                "kpss_stationary": is_kpss_stat,
                "verdict": verdict,
            }
        )

    return pd.DataFrame(results)


def preprocess_and_check_stationarity(
    df: pd.DataFrame,
    tol_mean: float = 1e-1,
    tol_std: float = 1e-1,
    alpha: float = 0.05,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, bool]:
    """Runs the full preprocessing and stationarity check pipeline.

    1) Extract raw and z-scored columns.
    2) Z-score raw columns if needed.
    3) Build the full standardized dataset.
    4) Run ADF+KPSS on selected standardized columns.

    Args:
        df (pd.DataFrame): Input DataFrame.
        tol_mean (float): Tolerance for mean check (default 0.1).
        tol_std (float): Tolerance for std check (default 0.1).
        alpha (float): Significance level for stationarity tests (default 0.05).

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, bool]:
            - full_z_df (pd.DataFrame): Combined standardized DataFrame.
            - norm_stats (pd.DataFrame): Statistics of raw column normalization.
            - stat_report (pd.DataFrame): Stationarity test results.
            - already_standardized (bool): True if raw columns were already standardized.
    """
    # 1. Identify raw vs z-scored columns
    all_cols = df.columns
    z_cols_existing = [c for c in all_cols if str(c).endswith("_z")]
    raw_cols = [
        c for c in all_cols if c not in ["date"] and not str(c).endswith("_z")
    ]

    # --- MODIFICATION 1: Verify existing _z columns ---
    if z_cols_existing:
        z_stats = df[z_cols_existing].agg(["mean", "std"]).T
        z_stats["mean_ok"] = z_stats["mean"].abs() < 0.1
        z_stats["std_ok"] = (z_stats["std"] - 1).abs() < 0.1

        logger.info("--- Verification of existing _z columns ---")
        # Log the dataframe as a string
        logger.info("\n%s", z_stats)

        if not z_stats["mean_ok"].all() or not z_stats["std_ok"].all():
            logger.warning(
                "WARNING: Some existing _z columns do not appear to be standardized!"
            )
        else:
            logger.info("All existing _z columns appear to be correctly standardized.")
        logger.info("-------------------------------------------")

    # 2. Z-score raw columns
    z_df_raw, norm_stats, already_std = zscore_check_and_apply(
        df, raw_cols, tol_mean=tol_mean, tol_std=tol_std
    )

    # 3. Build full standardized dataset
    full_z_df = pd.concat([z_df_raw, df[z_cols_existing]], axis=1)

    # --- MODIFICATION 2: Choose a single set of standardized features for ADF/KPSS ---
    # Option A (Recommended): Use only the existing `_z` columns
    stat_cols = z_cols_existing

    # --- MODIFICATION 3: Update the output accordingly ---
    logger.info(
        "Selected %d columns for stationarity testing (using existing _z columns).",
        len(stat_cols),
    )

    # 4. Run ADF+KPSS on selected standardized columns
    stat_report = stationarity_report(full_z_df, stat_cols, alpha=alpha)

    return full_z_df, norm_stats, stat_report, already_std


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Example usage
    try:
        data_path = "data/processed/features_dataset.parquet"
        df = pd.read_parquet(data_path)
        # Ensure date is index if it exists as column
        if "date" in df.columns:
            df = df.set_index("date")

        full_z_df, norm_stats, stat_report, already_std = (
            preprocess_and_check_stationarity(df)
        )

        logger.info("Normalization Stats (Raw Columns):")
        logger.info("\n%s", norm_stats)
        logger.info("Stationarity Report:")
        logger.info("\n%s", stat_report)
        logger.info("Already Standardized: %s", already_std)

    except FileNotFoundError:
        logger.error(
            "features_dataset.parquet not found. Please ensure the file is in the working directory."
        )
