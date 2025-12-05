import logging
import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller, kpss

# Configure logging for module-level usage if imported, though usually configured at entry point
logger = logging.getLogger(__name__)


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
    """Runs the stationarity check pipeline on existing z-scored columns.
    
    1) Identifies _z columns.
    2) Runs ADF + KPSS on those columns.
    3) Returns the original DataFrame and the stationarity report.

    Args:
        df (pd.DataFrame): Input DataFrame.
        tol_mean (float): Unused, kept for compatibility.
        tol_std (float): Unused, kept for compatibility.
        alpha (float): Significance level for stationarity tests (default 0.05).

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, bool]:
            - df (pd.DataFrame): Original DataFrame (no standardization applied here).
            - norm_stats (pd.DataFrame): Empty DataFrame (kept for compatibility).
            - stat_report (pd.DataFrame): Stationarity test results.
            - already_standardized (bool): Always True.
    """
    # 1. Identify z-scored columns
    all_cols = df.columns
    z_cols_existing = [c for c in all_cols if str(c).endswith("_z")]

    # 2. Log info
    logger.info("Selected %d z_ columns for stationarity testing.", len(z_cols_existing))

    # 3. Run ADF+KPSS on selected columns
    stat_report = stationarity_report(df, z_cols_existing, alpha=alpha)

    # 4. Return compatible args
    norm_stats = pd.DataFrame(columns=["feature", "mean", "std"])
    already_standardized = True

    return df, norm_stats, stat_report, already_standardized


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

        df, norm_stats, stat_report, already_std = (
            preprocess_and_check_stationarity(df)
        )

        logger.info("Stationarity checks completed on _z columns.")
        logger.info("Stationarity Report:")
        logger.info("\n%s", stat_report)

    except FileNotFoundError:
        logger.error(
            "features_dataset.parquet not found. Please ensure the file is in the working directory."
        )
