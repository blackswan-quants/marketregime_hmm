import logging

import matplotlib.pyplot as plt
import numpy as np
from hmmlearn import hmm
from matplotlib.patches import Patch

logger = logging.getLogger(__name__)

np.random.seed(0)
plt.style.use("seaborn-v0_8-darkgrid")


# --- 1. Bivariate Data Generation (Return, Volatility) ---
def generate_2d_synthetic_data(n_samples=2000):
    """
    Generates 2D synthetic observations: [Log-Returns, Log-Volatility].

    This function simulates a Hidden Markov Model (HMM) process with 3 distinct regimes:
    - Bull: Positive returns, low volatility.
    - Bear: Negative returns, high volatility.
    - Sideways: Near-zero returns, compressed volatility.

    Args:
        n_samples (int): Number of samples to generate.

    Returns:
        tuple: (X, Z, model)
            - X (np.ndarray): 2D array of observations (n_samples, 2).
            - Z (np.ndarray): Array of true hidden states (n_samples,).
            - model (hmm.GaussianHMM): The HMM model instance used for generation.
    """
    model = hmm.GaussianHMM(n_components=3, covariance_type="full")

    model.startprob_ = np.array([0.5, 0.2, 0.3])
    # Transition matrix columns: Bull, Bear, Sideways
    model.transmat_ = np.array(
        [[0.96, 0.01, 0.03], [0.02, 0.90, 0.08], [0.02, 0.05, 0.93]]  # From Bull  # From Bear  # From Sideways
    )

    # REGIME DEFINITION (Feature 0: Log-Returns, Feature 1: Log-Volatility)
    # Note: Log-Vol is used because volatility is strictly positive and log-normal.

    # Bull: Positive returns, Low Volatility (very negative Log-Vol)
    bull_mean = [0.0010, -5.5]

    # Bear: Negative returns, High Volatility (less negative/higher Log-Vol)
    bear_mean = [-0.0015, -3.5]

    # Sideways: Zero returns, "Compressed" Volatility (Very low, typical pre-breakout)
    side_mean = [0.0000, -6.5]

    model.means_ = np.array([bull_mean, bear_mean, side_mean])

    # Covariances (3 components, 2x2 matrix)
    # Bull: low variance in returns, low variance in vol
    cov_bull = [[0.00002, 0.0], [0.0, 0.1]]
    # Bear: high variance in returns, high variance in vol (panic mode)
    cov_bear = [[0.00020, -0.001], [-0.001, 0.5]]  # Negative correlation (leverage effect)
    # Sideways: medium return variance, very low vol variance (stability)
    cov_side = [[0.00001, 0.0], [0.0, 0.05]]

    model.covars_ = np.array([cov_bull, cov_bear, cov_side])

    X, Z = model.sample(n_samples)
    return X, Z, model


# --- 2. Fit Multivariate HMM ---
def fit_2d_model(X):
    """
    Fits a Gaussian HMM on the provided 2D data.

    Args:
        X (np.ndarray): The 2D observation array.

    Returns:
        tuple: (model, Z_pred)
            - model (hmm.GaussianHMM): The trained HMM model.
            - Z_pred (np.ndarray): The sequence of predicted hidden states.
    """
    model = hmm.GaussianHMM(n_components=3, covariance_type="full", n_iter=200, random_state=42, init_params="stmc")
    model.fit(X)
    _, Z_pred = model.decode(X)
    return model, Z_pred


# --- 3. Semantic Mapping 2D ---
def map_states_2d(model):
    """
    Maps the unsupervised HMM states to semantic labels (Bull, Bear, Sideways) based on their statistical properties.

    Logic:
    1. Identify Bear state: Highest Log-Volatility.
    2. Identify Bull state: Highest Mean Return among the remaining.
    3. Identify Sideways state: The remaining state.

    Args:
        model (hmm.GaussianHMM): The trained HMM model.

    Returns:
        dict: A mapping dictionary where keys are state IDs and values are dicts with 'label' and 'color'.
    """
    means_ret = model.means_[:, 0]
    means_vol = model.means_[:, 1]

    mapping = {}
    ids = [0, 1, 2]

    # Find Bear (Max Volatility)
    bear_id = np.argmax(means_vol)
    mapping[bear_id] = {"label": "Bear", "color": "#e74c3c"}  # Red
    ids.remove(bear_id)

    # Amongst remaining, find Bull (Max Return)
    if means_ret[ids[0]] > means_ret[ids[1]]:
        bull_id = ids[0]
        side_id = ids[1]
    else:
        bull_id = ids[1]
        side_id = ids[0]

    mapping[bull_id] = {"label": "Bull", "color": "#2ecc71"}  # Green
    mapping[side_id] = {"label": "Sideways", "color": "#95a5a6"}  # Gray

    logger.info("--- Identified Semantic Mapping (Automatic) ---")
    for i in range(3):
        logger.info(
            "State %s: %s | Mean Ret: %.5f | Mean LogVol: %.2f",
            i,
            mapping[i]["label"],
            means_ret[i],
            means_vol[i],
        )

    return mapping


# --- 4. Advanced Plotting ---
def plot_results_2d(X, Z_true, Z_pred, mapping):
    """
    Visualizes the results of the HMM analysis, including price series with regime overlays,
    feature space clustering, and regime alignment accuracy.

    Args:
        X (np.ndarray): The observation data.
        Z_true (np.ndarray): True hidden states (from synthetic generation).
        Z_pred (np.ndarray): Predicted hidden states.
        mapping (dict): Semantic mapping for state colors and labels.
    """
    # Reconstruct Synthetic Price (using only the returns column X[:,0])
    price = 100 * np.exp(np.cumsum(X[:, 0]))

    fig = plt.figure(figsize=(14, 10))
    gs = fig.add_gridspec(2, 2)

    # PLOT 1: Time Series (Price)
    ax1 = fig.add_subplot(gs[0, :])
    ax1.plot(price, color="black", lw=1)
    ax1.set_title("Asset Price & Predicted Regimes (Multivariate HMM)", fontsize=14)

    start_idx = 0
    for i in range(1, len(Z_pred)):
        if Z_pred[i] != Z_pred[i - 1]:
            ax1.axvspan(start_idx, i, color=mapping[Z_pred[start_idx]]["color"], alpha=0.3, lw=0)
            start_idx = i
    ax1.axvspan(start_idx, len(Z_pred), color=mapping[Z_pred[start_idx]]["color"], alpha=0.3, lw=0)

    # Sorted Legend
    patches = [Patch(color=v["color"], label=v["label"], alpha=0.3) for k, v in mapping.items()]
    # Visual order: Bear, Sideways, Bull
    sorted_patches = sorted(patches, key=lambda x: ["Bear", "Sideways", "Bull"].index(x.get_label()))
    ax1.legend(handles=sorted_patches, loc="upper left")

    # PLOT 2: Scatter Plot (Return vs Volatility)
    # This shows the Gaussian clusters in feature space
    ax2 = fig.add_subplot(gs[1, 0])

    # Scatter of points colored by prediction
    colors = [mapping[z]["color"] for z in Z_pred]
    ax2.scatter(X[:, 0], X[:, 1], c=colors, s=5, alpha=0.5)

    ax2.set_title("Feature Space Clustering", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Log Returns")
    ax2.set_ylabel("Log Volatility")

    # PLOT 3: Accuracy (Visual Confusion Matrix strips)
    # Compare True vs Pred strips
    ax3 = fig.add_subplot(gs[1, 1])

    # Map numeric IDs to RGB colors for imshow
    def to_rgb(z_seq):
        rgb_seq = []
        for z in z_seq:
            hex_col = mapping[z]["color"].lstrip("#")
            rgb_seq.append(tuple(int(hex_col[i : i + 2], 16) / 255.0 for i in (0, 2, 4)))
        return np.array(rgb_seq)

    rgb_true = to_rgb(Z_true)
    # Note: reusing predicted colors for Z_true assumes semantic alignment was correct.
    # In a real scenario, Z_true should be explicitly mapped if labels differ.
    rgb_pred = to_rgb(Z_pred)

    ax3.imshow(rgb_true[np.newaxis, :], aspect="auto", extent=[0, len(Z_true), 0.5, 1])
    ax3.imshow(rgb_pred[np.newaxis, :], aspect="auto", extent=[0, len(Z_true), 0, 0.5])

    ax3.set_yticks([0.25, 0.75])
    ax3.set_yticklabels(["Model", "Truth"])
    ax3.set_title("Regime Alignment (Truth vs Model)", fontsize=12, fontweight="bold")

    plt.tight_layout()
    plt.show()


# --- Execution ---
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # 1. Generate (Returns, Vol)
    X_2d, Z_true, _ = generate_2d_synthetic_data(2000)

    # 2. Fit
    model_2d, Z_pred = fit_2d_model(X_2d)

    # 3. Map
    try:
        mapping = map_states_2d(model_2d)

        # 4. Plot
        plot_results_2d(X_2d, Z_true, Z_pred, mapping)
    except Exception:
        logger.exception("Error in mapping (likely convergence failure)")
