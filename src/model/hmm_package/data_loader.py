import logging
import os

import numpy as np
import pandas as pd
from hmmlearn import hmm
from scipy.stats import skewnorm, t

logger = logging.getLogger(__name__)


def load_real_data(filepath):
    """
    Load real market data from a parquet file and prepare it for HMM analysis.

    Supports dynamic feature selection:
    1. PCA Features: If columns contain 'PC' (e.g., market_PC1), they are prioritized.
    2. Fallback Features: If no PCA columns, falls back to raw indicators:
       - Log-Returns (from spx_close)
       - Momentum (R1)
       - Log-Volatility (V1)
       - Drawdown (D1)
       - Yield Curve (M1)

    Args:
        filepath (str): Path to the .parquet data file.

    Returns:
        tuple: (X, Z_true, feature_names, aux_data)
            - X (np.ndarray): Feature matrix (n_samples, n_features).
            - Z_true (np.ndarray): Dummy zeros array (since real data has no ground truth).
            - feature_names (list): List of selected feature names.
            - aux_data (dict): Dictionary with auxiliary data like 'date' and 'price'.

    Raises:
        FileNotFoundError: If the filepath does not exist.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    if filepath.endswith(".parquet"):
        df = pd.read_parquet(filepath)
    else:
        df = pd.read_csv(filepath)

    logger.info("Loaded data columns: %s", df.columns.tolist())

    # Ensure date is sorted and handled
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
    elif isinstance(df.index, pd.DatetimeIndex):
        # If date is in index, move it to a column
        df = df.copy()
        if df.index.name == "date":
            df.index.name = "date_index"
        df["date"] = df.index
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

    features = []
    feature_names = []

    # Check for PCA features first
    pc_cols = sorted([c for c in df.columns if "PC" in c])

    if len(pc_cols) > 0:
        logger.info("Using PCA features: %s", pc_cols)
        df = df.dropna(subset=pc_cols)
        for col in pc_cols:
            features.append(df[col].values)
            feature_names.append(col)
    else:
        # Fallback to original logic - Use specific column names if they exist
        # Log-Returns
        if "spx_close" in df.columns:
            df["log_ret"] = np.log(df["spx_close"] / df["spx_close"].shift(1))
            df = df.dropna(subset=["log_ret"])
            features.append(df["log_ret"].values)
            feature_names.append("Log-Returns")
        elif "R1" in df.columns:
            df = df.dropna(subset=["R1"])
            features.append(df["R1"].values)
            feature_names.append("Log-Returns")  # Keep naming consistent with user expectation

        # Log-Volatility
        if "V1" in df.columns:
            df = df.dropna(subset=["V1"])
            features.append(np.log(df["V1"].values + 1e-6))
            feature_names.append("Log-Volatility")
        elif "vix_close" in df.columns:
            df = df.dropna(subset=["vix_close"])
            features.append(np.log(df["vix_close"].values + 1e-6))
            feature_names.append("Log-Volatility")

        # Drawdown
        if "D1" in df.columns:
            df = df.dropna(subset=["D1"])
            features.append(df["D1"].values)
            feature_names.append("Drawdown")

        # Yield Curve
        if "M1" in df.columns:
            df = df.dropna(subset=["M1"])
            features.append(df["M1"].values)
            feature_names.append("Yield Curve")

    if not features:
        raise ValueError(
            "No usable features found for HMM input. "
            "Expected PCA columns containing 'PC' or one of: spx_close, R1, V1, D1, M1. "
            f"Available columns: {df.columns.tolist()}"
        )

    X = np.column_stack(features)

    # Create dummy Z_true since we don't have labels
    Z_true = np.zeros(len(X), dtype=int)

    # Aux data (Dates, Price if available)
    aux_data = {}
    if "date" in df.columns:
        aux_data["date"] = df["date"].values

    # Search for a price column
    price_col = next((c for c in ["spx_close", "market_PC1", "price"] if c in df.columns), None)
    if price_col:
        aux_data["price"] = df[price_col].values
    else:
        # Fallback: cumulative sum of first feature if it looks like returns
        aux_data["price"] = 100 * np.exp(np.cumsum(X[:, 0] / 100.0))

    return X, Z_true, feature_names, aux_data


def generate_multifeature_data(n_samples=2000, noise_level=0.0, random_state=42):
    """
    Generate synthetic 4D multivariate market data with realistic, OVERLAPPING distributions.
    """
    model = hmm.GaussianHMM(n_components=3, covariance_type="full", random_state=random_state)

    model.startprob_ = np.array([0.5, 0.2, 0.3])

    # Sticky transitions (realistic persistence)
    model.transmat_ = np.array(
        [
            [0.96, 0.01, 0.03],  # Bull persists (~25 days avg)
            [0.02, 0.90, 0.08],  # Bear persists (~10 days avg)
            [0.02, 0.05, 0.93],  # Sideways persists (~14 days avg)
        ]
    )

    # OVERLAPPING means (hard mode)
    # [Return, Log-Vol, Drawdown, Yield_Curve]
    model.means_ = np.array(
        [
            [0.0002, -5.0, -0.02, 0.015],  # Bull
            [-0.0002, -4.0, -0.15, -0.005],  # Bear (vol close to Bull!)
            [0.0000, -6.0, -0.05, 0.002],  # Sideways
        ]
    )

    # Wide covariances (creates overlap)
    cov_bull = np.array(
        [
            [0.00005, 0.0, 0.0, 0.0],
            [0.0, 0.30, 0.0, 0.0],
            [0.0, 0.0, 0.0005, 0.0],
            [0.0, 0.0, 0.0, 0.00001],
        ]
    )

    cov_bear = np.array(
        [
            [0.00025, -0.002, -0.0001, 0.00001],
            [-0.002, 0.80, 0.02, -0.001],
            [-0.0001, 0.02, 0.002, -0.0001],
            [0.00001, -0.001, -0.0001, 0.00002],
        ]
    )

    cov_side = np.array(
        [
            [0.00002, 0.0, 0.0, 0.0],
            [0.0, 0.15, 0.0, 0.0],
            [0.0, 0.0, 0.0003, 0.0],
            [0.0, 0.0, 0.0, 0.00001],
        ]
    )

    model.covars_ = np.array([cov_bull, cov_bear, cov_side])

    X, Z_true = model.sample(n_samples)

    # Add measurement noise
    if noise_level > 0:
        feature_stds = X.std(axis=0)
        noise = np.random.randn(n_samples, 4) * feature_stds * noise_level
        X = X + noise

    feature_names = ["Log-Returns", "Log-Volatility", "Drawdown", "Yield Curve"]

    # Generate dummy dates
    dates = pd.date_range(start="2020-01-01", periods=n_samples, freq="D")
    aux_data = {"date": dates.values, "price": 100 * np.exp(np.cumsum(X[:, 0]))}  # Reconstruct price from returns

    return X, Z_true, feature_names, aux_data


def generate_broken_data(n_samples=2000):
    """
    Generates data that VIOLATES Gaussian assumptions (Fat Tails, Skewness).
    """
    # 1. Transition Matrix (Markovian dynamics remain)
    transmat = np.array(
        [[0.95, 0.02, 0.03], [0.05, 0.85, 0.10], [0.02, 0.03, 0.95]]  # Bull  # Bear (less stable)  # Sideways
    )

    # Initial states
    start_prob = np.array([0.4, 0.2, 0.4])

    # Generate state sequence
    Z = np.zeros(n_samples, dtype=int)
    Z[0] = np.random.choice(3, p=start_prob)
    for i in range(1, n_samples):
        Z[i] = np.random.choice(3, p=transmat[Z[i - 1]])

    X = np.zeros((n_samples, 4))

    # 2. Generate NON-GAUSSIAN Observations
    for i in range(n_samples):
        state = Z[i]

        if state == 0:  # BULL: Skewed Positive
            # Skew-Normal: returns rise slowly (long right tail)
            r = skewnorm.rvs(a=4, loc=0.0005, scale=0.008)
            v = np.random.normal(-5.0, 0.2)
            d = np.random.normal(-0.02, 0.01)
            y = np.random.normal(0.015, 0.002)

        elif state == 1:  # BEAR: Fat Tails (Student-t)
            # t-Student with low df=2.5 generates massive outliers
            r = t.rvs(df=2.5, loc=-0.002, scale=0.015)
            v = np.random.normal(-3.5, 0.8)
            d = np.random.normal(-0.15, 0.05)
            y = np.random.normal(-0.005, 0.005)

        elif state == 2:  # SIDEWAYS: Gaussian (Control)
            r = np.random.normal(0, 0.005)
            v = np.random.normal(-6.0, 0.1)
            d = np.random.normal(-0.05, 0.01)
            y = np.random.normal(0.002, 0.001)

        X[i] = [r, v, d, y]

    feature_names = ["Log-Returns", "Log-Volatility", "Drawdown", "Yield Curve"]

    dates = pd.date_range(start="2020-01-01", periods=n_samples, freq="D")
    aux_data = {"date": dates.values, "price": 100 * np.exp(np.cumsum(X[:, 0]))}

    return X, Z, feature_names, aux_data
