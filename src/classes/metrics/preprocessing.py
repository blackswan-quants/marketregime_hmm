import logging

import numpy as np
import pandas as pd
from scipy.stats import kurtosis, skew
from sklearn.decomposition import PCA
from statsmodels.stats.outliers_influence import variance_inflation_factor
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
        with np.errstate(all="ignore"):  # Catch warnings as errors if strict, but here just run
            # Note: kpss can issue warnings for interpolation
            stat, p_value, lags, crit = kpss(clean_series, regression=regression, nlags="auto")
        return {
            "stat": stat,
            "pvalue": p_value,
            "nobs": len(clean_series),
            "valid": True,
        }
    except Exception:
        return {"stat": np.nan, "pvalue": np.nan, "nobs": len(series), "valid": False}


def stationarity_report(df: pd.DataFrame, zscore_cols: list[str], alpha: float = 0.05) -> pd.DataFrame:
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


def compute_distribution_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute skewness and kurtosis for each feature in the DataFrame.
    Kurtosis is Fisher's kurtosis (normal ==> 0.0).
    """
    stats = []
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    for col in numeric_cols:
        data = df[col].dropna()
        if len(data) < 3:
            continue

        sk = skew(data)
        ku = kurtosis(data)

        stats.append(
            {
                "feature": col,
                "skewness": sk,
                "kurtosis": ku,
                "is_approx_normal": (abs(sk) < 1.0) and (abs(ku) < 2.0),
            }
        )

    df_stats = pd.DataFrame(stats)

    # Check for significant non-Gaussian properties
    non_normal = df_stats[~df_stats["is_approx_normal"]]
    if not non_normal.empty:
        logger.warning(
            "The following features show significant non-Gaussian properties (Skew > 1 or Excess Kurtosis > 2):"
        )
        logger.warning("\n%s", non_normal[["feature", "skewness", "kurtosis"]])
        logger.warning(
            "HMM assumes Gaussian emissions. Consider transformations (e.g., Yeo-Johnson) "
            "for these features if model performance is poor."
        )
    else:
        logger.info("All features appear approximately Gaussian.")

    return df_stats


def compute_vif(df: pd.DataFrame, threshold: float = 5.0) -> pd.DataFrame:
    """
    Calculate Variance Inflation Factor (VIF) for each feature.
    """
    df_numeric = df.select_dtypes(include=[np.number]).dropna()
    vif_data = pd.DataFrame()
    vif_data["feature"] = df_numeric.columns

    # Use values directly for VIF calculation
    X = df_numeric.values
    vif_values = []

    for i in range(len(df_numeric.columns)):
        try:
            val = variance_inflation_factor(X, i)
            vif_values.append(val)
        except Exception:
            vif_values.append(np.nan)

    vif_data["VIF"] = vif_values
    vif_data["high_collinearity"] = vif_data["VIF"] > threshold

    return vif_data.sort_values(by="VIF", ascending=False)


def reduce_collinearity_via_vif(df: pd.DataFrame, threshold: float = 5.0) -> tuple[pd.DataFrame, list[str]]:
    """
    Iteratively removes features with VIF > threshold.
    Removes the feature with the highest VIF one by one and recomputes VIF.

    Args:
        df (pd.DataFrame): Input dataframe (numeric columns only).
        threshold (float): VIF threshold (default 5.0).

    Returns:
        tuple[pd.DataFrame, list[str]]:
            - df_reduced: DataFrame with high-VIF features removed.
            - dropped_features: List of column names that were removed.
    """
    df_reduced = df.copy().select_dtypes(include=[np.number])
    dropped_features = []

    while True:
        # Calculate VIF for current features
        features = df_reduced.columns
        if len(features) < 2:
            break

        X = df_reduced.values
        vif_values = []
        for i in range(len(features)):
            try:
                val = variance_inflation_factor(X, i)
                vif_values.append(val)
            except Exception:
                vif_values.append(0.0)

        max_vif = max(vif_values)
        if max_vif > threshold:
            # Drop feature with max VIF
            max_idx = vif_values.index(max_vif)
            feature_to_drop = features[max_idx]
            df_reduced = df_reduced.drop(columns=[feature_to_drop])
            dropped_features.append(feature_to_drop)
            logger.info(f"Dropped {feature_to_drop} (VIF={max_vif:.2f})")
        else:
            break

    return df_reduced, dropped_features


def run_pca(
    df: pd.DataFrame,
    n_components: float | int = 0.80,
    columns_to_include: list[str] | None = None,
) -> tuple[PCA, pd.DataFrame, pd.DataFrame]:
    """
    Run PCA on the dataframe.

    Args:
        df: Input dataframe
        n_components: If float between 0 and 1, select components to explain variance.
                      If int >= 1, select that many components.
        columns_to_include: List of columns to use. If None, use all numeric.

    Returns:
        tuple containing:
            - factor_scores (pd.DataFrame): PC scores
            - explained_variance (np.ndarray): Explained variance ratios
            - loadings (pd.DataFrame): Principal component loadings
    """
    if columns_to_include:
        data = df[columns_to_include].dropna()
    else:
        data = df.select_dtypes(include=[np.number]).dropna()

    if data.empty:
        raise ValueError("No data available for PCA")

    pca = PCA(n_components=n_components)
    components = pca.fit_transform(data)

    # Create column names
    n_comps = components.shape[1]
    cols = [f"PC{i+1}" for i in range(n_comps)]

    factor_scores = pd.DataFrame(components, index=data.index, columns=cols)
    loadings = pd.DataFrame(pca.components_.T, index=data.columns, columns=cols)

    return pca, factor_scores, loadings


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

        df, norm_stats, stat_report, already_std = preprocess_and_check_stationarity(df)

        logger.info("Stationarity checks completed on _z columns.")
        logger.info("Stationarity Report:")
        logger.info("\n%s", stat_report)

        # --- New Feature Analysis Logic ---
        logger.info("--- Starting Distribution Analysis ---")
        dist_stats = compute_distribution_stats(df)
        logger.info("\n%s", dist_stats)

        logger.info("--- Starting VIF Analysis ---")
        vif_stats = compute_vif(df)
        logger.info("\n%s", vif_stats)

        logger.info("--- Starting PCA Analysis ---")
        # Example: Run PCA on all Z-scored columns
        z_cols = [c for c in df.columns if c.endswith("_z")]
        if z_cols:
            pca, factor_scores, loadings = run_pca(df, n_components=0.80, columns_to_include=z_cols)
            logger.info("PCA Explained Variance: %s", pca.explained_variance_ratio_)
            logger.info("Factor Scores Shape: %s", factor_scores.shape)
        else:
            logger.warning("No _z columns found for PCA.")

    except FileNotFoundError:
        logger.error("features_dataset.parquet not found. Please ensure the file is in the working directory.")
