import os
import sys

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import joblib
from sklearn.preprocessing import StandardScaler

from src.model.hmm_package.config import DATA_TYPE, MODEL_SAVE_PATH, N_SAMPLES, NOISE_LEVEL, REAL_DATA_PATH
from src.model.hmm_package.data_loader import generate_broken_data, generate_multifeature_data, load_real_data
from src.model.hmm_package.model import select_best_model, train_hmm
from src.model.hmm_package.vis import create_dashboard, plot_model_selection


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
    print("=" * 60)
    print("HMM MARKET REGIME DETECTION PIPELINE")
    print("=" * 60)

    # 1. Load Data
    # 1. Load Data
    if DATA_TYPE == "Gaussian":
        print(">>> Generating Standard Gaussian Data...")
        X_raw, Z_true, feature_names, aux_data = generate_multifeature_data(N_SAMPLES, NOISE_LEVEL)
    elif DATA_TYPE == "Broken":
        print(">>> Generating Broken/Non-Gaussian Data...")
        X_raw, Z_true, feature_names, aux_data = generate_broken_data(N_SAMPLES)
    elif DATA_TYPE == "Real":
        print(f">>> Loading Real Data from {REAL_DATA_PATH}...")
        try:
            X_raw, Z_true, feature_names, aux_data = load_real_data(REAL_DATA_PATH)
        except FileNotFoundError:
            print(f"Error: {REAL_DATA_PATH} not found. Using Synthetic Data instead.")
            X_raw, Z_true, feature_names, aux_data = generate_multifeature_data(N_SAMPLES, NOISE_LEVEL)
            # data_type = "Gaussian" # Keep as Real to test pipeline flow or change logic
    else:
        raise ValueError(f"Unknown DATA_TYPE: {DATA_TYPE}")

    print(f"Loaded {len(X_raw)} samples with {len(feature_names)} features.")

    # 2. Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)

    # 3. Model Selection

    # Check if a model already exists
    # Check if a model already exists
    if os.path.exists(MODEL_SAVE_PATH):
        print(f"\nFound existing model at: {MODEL_SAVE_PATH}")
        # Automatically retrain to ensure fresh results
        print(">>> Retraining model to ensure fresh visualizations...")

    # If using Real Data, we might want to find optimal K
    # If using Synthetic, we know K=3 but can test anyway

    best_k_auto, df_results = select_best_model(X_scaled, min_k=2, max_k=10)

    # Plot AIC/BIC
    plot_model_selection(df_results)

    # Ask User for K (Restored)
    print(f"\nAuto-detected optimal k: {best_k_auto}")
    print("Check 'model_selection.png' for the BIC plot.")
    user_k = input(f"Enter number of regimes to use (default {best_k_auto}): ").strip()

    if user_k.isdigit():
        best_k = int(user_k)
    else:
        best_k = best_k_auto

    # 4. Final Training
    print(f"\nTraining final model with k={best_k} regimes...")
    model, Z_pred, logprob = train_hmm(X_scaled, n_components=best_k)

    # Calculate Posterior Probabilities
    print("Computing posterior probabilities...")
    posterior_probs = model.predict_proba(X_scaled)

    # Save Model
    print(f"Saving model to {MODEL_SAVE_PATH}...")
    joblib.dump(model, MODEL_SAVE_PATH)

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
    )


if __name__ == "__main__":
    run_pipeline()
