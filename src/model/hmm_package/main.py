
import sys
import os

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.model.hmm_package.config import *
from src.model.hmm_package.data_loader import load_real_data, generate_multifeature_data, generate_broken_data
from src.model.hmm_package.model import select_best_model, train_hmm
from src.model.hmm_package.vis import create_dashboard, plot_model_selection
from sklearn.preprocessing import StandardScaler
import numpy as np

def run_pipeline():
    print("=" * 60)
    print("HMM MARKET REGIME DETECTION PIPELINE")
    print("=" * 60)
    
    # 1. Load Data
    if DATA_TYPE == "Gaussian":
        print(">>> Generating Standard Gaussian Data...")
        X_raw, Z_true, feature_names = generate_multifeature_data(N_SAMPLES, NOISE_LEVEL)
    elif DATA_TYPE == "Broken":
        print(">>> Generating Broken/Non-Gaussian Data...")
        X_raw, Z_true, feature_names = generate_broken_data(N_SAMPLES)
    elif DATA_TYPE == "Real":
        print(f">>> Loading Real Data from {REAL_DATA_PATH}...")
        try:
            X_raw, Z_true, feature_names = load_real_data(REAL_DATA_PATH)
        except FileNotFoundError:
            print(f"Error: {REAL_DATA_PATH} not found. Using Synthetic Data instead.")
            X_raw, Z_true, feature_names = generate_multifeature_data(N_SAMPLES, NOISE_LEVEL)
            # data_type = "Gaussian" # Keep as Real to test pipeline flow or change logic
    else:
        raise ValueError(f"Unknown DATA_TYPE: {DATA_TYPE}")
        
    print(f"Loaded {len(X_raw)} samples with {len(feature_names)} features.")
    
    # 2. Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)
    
    # 3. Model Selection
    # If using Real Data, we might want to find optimal K
    # If using Synthetic, we know K=3 but can test anyway
    
    best_k_auto, df_results = select_best_model(X_scaled, min_k=2, max_k=10)
    
    # Plot AIC/BIC
    plot_model_selection(df_results)
    
    # Ask User for K
    print(f"\nAuto-detected optimal k: {best_k_auto}")
    user_k = input(f"Enter number of regimes to use (default {best_k_auto}): ").strip()
    
    if user_k.isdigit():
        best_k = int(user_k)
    else:
        best_k = best_k_auto
    
    # 4. Final Training
    print(f"\nTraining final model with k={best_k} regimes...")
    model, Z_pred, logprob = train_hmm(X_scaled, n_components=best_k)
    
    # 5. Visualize
    create_dashboard(X_raw, Z_pred, feature_names, logprob, data_type=DATA_TYPE, Z_true=Z_true)
    
if __name__ == "__main__":
    run_pipeline()
