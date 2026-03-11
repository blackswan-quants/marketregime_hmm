import os

DATA_TYPE = "Real"  # Options: "Gaussian", "Broken", "Real"
REAL_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../data/processed/market_features.parquet")
)
NOISE_LEVEL = 0.15
N_SAMPLES = 2000
N_REGIMES = 3  # Default k, will be overridden by selection
RANDOM_STATE = 42


MODEL_SAVE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hmm_model.pkl")

REGIME_NAMES = {0: "Bull", 1: "Bear", 2: "Sideways"}
REGIME_COLORS = {"Bull": "#2ecc71", "Bear": "#e74c3c", "Sideways": "#95a5a6"}
