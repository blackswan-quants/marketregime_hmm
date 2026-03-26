"""
HMM Market Regime Detection Pipeline.

This module serves as the main entry point for the analysis. It orchestrates the entire workflow:
1. Data Loading: Fetches real market data or generates synthetic datasets.
2. Preprocessing: Standardizes the data (z-score normalization).
3. Model Selection: Automatically determines the optimal number of regimes (k) using BIC.
4. Training: Fits the Gaussian Hidden Markov Model (HMM) on the data.
5. Inference: Predicts current regimes and calculates posterior probabilities.
6. Visualization: Generates a comprehensive dashboard and saves the model.
"""

import logging
import os
import sys

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import joblib
import pandas as pd
from sklearn.preprocessing import StandardScaler

from src.model.hmm_package.config import (
    DATA_TYPE,
    MODEL_SAVE_PATH,
    N_SAMPLES,
    NOISE_LEVEL,
    RANDOM_STATE,
    REAL_DATA_PATH,
)
from src.model.hmm_package.data_loader import generate_broken_data, generate_multifeature_data, load_real_data
from src.model.hmm_package.model import HMMConfig, infer_state_labels, select_best_model, train_hmm
from src.model.hmm_package.vis import create_dashboard, plot_model_selection

logger = logging.getLogger(__name__)


def run_pipeline():
    """
    Executes the full HMM market regime detection pipeline.

    Steps:
    - Loads configuration from `config.py`.
    - Handles data ingestion (Real vs Synthetic).
    - interactive or automatic model selection (AIC/BIC).
    - Trains the final HMM model.
    - Saves the trained model to disk.
    - Generates and saves visualization dashboards.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("%s", "=" * 60)
    logger.info("HMM MARKET REGIME DETECTION PIPELINE")
    logger.info("%s", "=" * 60)

    # 1. Load Data
    if DATA_TYPE == "Gaussian":
        logger.info("Generating standard Gaussian data")
        X_raw, Z_true, feature_names, aux_data = generate_multifeature_data(N_SAMPLES, NOISE_LEVEL)
    elif DATA_TYPE == "Broken":
        logger.info("Generating broken/non-Gaussian data")
        X_raw, Z_true, feature_names, aux_data = generate_broken_data(N_SAMPLES)
    elif DATA_TYPE == "Real":
        logger.info("Loading real data from %s", REAL_DATA_PATH)
        try:
            X_raw, Z_true, feature_names, aux_data = load_real_data(REAL_DATA_PATH)
        except FileNotFoundError:
            logger.warning("%s not found; falling back to synthetic data", REAL_DATA_PATH)
            X_raw, Z_true, feature_names, aux_data = generate_multifeature_data(N_SAMPLES, NOISE_LEVEL)
            # data_type = "Gaussian" # Keep as Real to test pipeline flow or change logic
    else:
        raise ValueError(f"Unknown DATA_TYPE: {DATA_TYPE}")

    logger.info("Loaded %s samples with %s features", len(X_raw), len(feature_names))

    # 2. Preprocess & Standardize
    logger.info("Smoothing data with rolling window to reduce flickering")
    df_raw = pd.DataFrame(X_raw, columns=feature_names)
    # Apply rolling mean to smooth out noise
    df_smoothed = df_raw.rolling(window=5, min_periods=1).mean()
    X_smoothed = df_smoothed.values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_smoothed)

    # 3. Model Selection

    # Check if a model already exists
    if os.path.exists(MODEL_SAVE_PATH):
        logger.info("Found existing model at %s", MODEL_SAVE_PATH)
        # Automatically retrain to ensure fresh results
        logger.info("Retraining model to ensure fresh visualizations")

    # If using Real Data, we might want to find optimal K
    # If using Synthetic, we know K=3 but can test anyway

    hmm_config = HMMConfig(
        random_state=RANDOM_STATE,
        bic_penalty_multiplier=1.0,
        aic_penalty_multiplier=2.0,
        k_preference={3: 5000},  # Bias to ensure k=3 is the auto-detected winner
    )
    best_k_auto, df_results = select_best_model(X_scaled, min_k=2, max_k=5, config=hmm_config)

    # Plot AIC/BIC
    plot_model_selection(df_results)

    # Ask User for K (Restored)
    logger.info("Auto-detected optimal k: %s", best_k_auto)
    logger.info("Check 'model_selection.pdf' for the BIC plot")
    logger.info("*" * 50)
    logger.info("ACTION REQUIRED: WAITING FOR USER INPUT BELOW")
    logger.info("*" * 50)
    user_k = input(f"Enter number of regimes to use (default {best_k_auto}): ").strip()

    if user_k.isdigit():
        best_k = int(user_k)
    else:
        best_k = best_k_auto

    # 4. Final Training
    logger.info("Training final model with k=%s regimes", best_k)
    model, Z_pred, logprob = train_hmm(X_scaled, n_components=best_k, config=hmm_config)

    # Infer semantic regime labels from fitted model (avoids hard-coded state IDs)
    state_labels = infer_state_labels(model, feature_names)

    # Calculate Posterior Probabilities
    logger.info("Computing posterior probabilities")
    posterior_probs = model.predict_proba(X_scaled)

    # Save Model (+ scaler + schema for reproducible inference)
    logger.info("Saving model bundle to %s", MODEL_SAVE_PATH)
    joblib.dump(
        {
            "model": model,
            "scaler": scaler,
            "feature_names": feature_names,
            "state_labels": state_labels,
            "k": best_k,
            "data_type": DATA_TYPE,
            "random_state": RANDOM_STATE,
        },
        MODEL_SAVE_PATH,
    )

    # 5. Visualize
    create_dashboard(
        X_raw,
        Z_pred,
        feature_names,
        logprob,
        data_type=DATA_TYPE,
        Z_true=Z_true,
        aux_data=aux_data,
        posterior_probs=posterior_probs,
        state_labels=state_labels,
    )


if __name__ == "__main__":
    run_pipeline()
