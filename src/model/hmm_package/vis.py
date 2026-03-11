import logging
import os

import matplotlib.cm as cm
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from .config import REGIME_NAMES

logger = logging.getLogger(__name__)

"""
Visualization Module for HMM Market Regime Detection.

This module provides comprehensive plotting utilities to analyze and interpret the results of the HMM.
It includes functions for:
- 3D Feature Space Clustering: Visualizing how regimes cluster in PCA space.
- Regime Characterization: Heatmaps and violin plots showing feature statistics per regime.
- Market Timeline: Overlaying predicted regimes on price/index charts.
- Model Selection: Plotting AIC/BIC curves.
"""


def plot_regime_bg(
    ax,
    Z,
    data,
    title,
    acc=None,
    state_labels=None,
    label_colors=None,
    ylabel="Value",
    dates=None,
):
    """Plot data series with colored regime backgrounds."""
    if state_labels is None:
        state_labels = {}
    if label_colors is None:
        label_colors = {}

    if dates is not None:
        ax.plot(dates, data, "k", lw=1)
    else:
        ax.plot(data, "k", lw=1)

    # Generate colormap for regimes dynamically if needed
    unique_states = np.unique(Z)
    cmap = cm.get_cmap("tab10" if len(unique_states) <= 10 else "tab20")

    # Build complete color mapping
    for idx, state in enumerate(unique_states):
        regime_name = state_labels.get(state, f"Regime_{state}")
        if regime_name not in label_colors:
            label_colors[regime_name] = cmap(idx % (10 if len(unique_states) <= 10 else 20))

    # Find regime change points
    changes = np.concatenate(([0], np.where(Z[:-1] != Z[1:])[0] + 1, [len(Z)]))
    for i in range(len(changes) - 1):
        s, e = changes[i], changes[i + 1]
        regime_name = state_labels.get(Z[s], f"Regime_{Z[s]}")
        color = label_colors.get(regime_name, "#95a5a6")

        if dates is not None:
            ax.axvspan(dates[s], dates[min(e, len(dates) - 1)], color=color, alpha=0.3, lw=0)
        else:
            ax.axvspan(s, e, color=color, alpha=0.3, lw=0)

    title_text = title
    if acc is not None:
        title_text += f" (Acc: {acc:.1%})"
    ax.set_title(title_text, fontweight="bold", fontsize=10)
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)

    if dates is not None:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")


# Descriptions based on PCA Loadings
FEATURE_DESCRIPTIONS = {
    "market_PC1": "Risk-Off (Short SPX, Long Vol)",
    "market_PC2": "Tail Risk (Vol-of-Vol)",
    "credit_PC1": "Credit Stress (Curve & Spread)",
    "credit_PC2": "Credit Structure (Slope)",
    "cross_asset_PC1": "Corr Structure (X-Asset)",
    "cross_asset_PC2": "Beta Structure",
    "cross_asset_PC3": "Mom/Value Factor",
}


def create_dashboard(
    X_raw,
    Z_pred,
    feature_names,
    logprob,
    data_type="Real",
    Z_true=None,
    aux_data=None,
    posterior_probs=None,
    state_labels=None,
    label_colors=None,
):
    """
    Generates a comprehensive analytical dashboard for the HMM model results.

    The dashboard consists of 4 main panels:
    1. Feature Space (3D/2D Scatter): Visualizes the separation of regimes in the Principal Component space.
    2. Regime Characteristics (Heatmap): Shows the mean z-score of each feature for every regime (interpretability).
    3. Feature Distributions (Violin Plot): detailed view of feature density per regime to check for outliers/overlaps.
    4. Market Timeline: The main time-series chart with regime overlays.

    Args:
        X_raw (np.ndarray): The raw (or scaled) feature matrix used for plotting.
        Z_pred (np.ndarray): Predicted hidden states sequence.
        feature_names (list): List of feature names corresponding to columns of X_raw.
        logprob (float): Final log-likelihood of the fitted model.
        data_type (str): "Real", "Gaussian", or "Broken" (for title context).
        Z_true (np.ndarray, optional): Ground truth states (for synthetic data validation).
        aux_data (dict, optional): Auxiliary data like timestamps or reference prices.
        posterior_probs (np.ndarray, optional): Posterior probabilities (not currently plotted but available).

    Saves:
        A 'latest_dashboard.png' image in the current directory.
    """
    # X_raw columns correspond to feature_names

    # Setup Figure
    fig = plt.figure(figsize=(24, 14))  # Widen figure
    gs = fig.add_gridspec(2, 3, height_ratios=[1, 1])

    title_extra = f" | LogL: {logprob:.0f}" if logprob else ""
    plt.suptitle(f"HMM Regime Detection ({data_type} Data){title_extra}", fontsize=18, fontweight="bold", y=0.98)

    # --- PLOT 1: Feature Space Clusters (3D Principal Components) ---
    if state_labels is None:
        state_labels = REGIME_NAMES
    if label_colors is None:
        label_colors = {}

    # Generate colors dynamically for all regimes
    unique_states = np.unique(Z_pred)
    cmap = cm.get_cmap("tab10" if len(unique_states) <= 10 else "tab20")

    # Build complete color mapping
    for idx, state in enumerate(unique_states):
        regime_name = state_labels.get(state, f"Regime_{state}")
        if regime_name not in label_colors:
            label_colors[regime_name] = cmap(idx % (10 if len(unique_states) <= 10 else 20))

    colors_pred = [label_colors.get(state_labels.get(z, f"Regime_{z}"), "#95a5a6") for z in Z_pred]

    if X_raw.shape[1] >= 3:
        ax_clust = fig.add_subplot(gs[0, 0], projection="3d")
        ax_clust.scatter(X_raw[:, 0], X_raw[:, 1], X_raw[:, 2], c=colors_pred, alpha=0.5, s=15, edgecolors="none")
        ax_clust.set_xlabel(feature_names[0])
        ax_clust.set_ylabel(feature_names[1])
        ax_clust.set_zlabel(feature_names[2])
        ax_clust.set_title("1. Feature Space (Top 3 PC)", fontweight="bold")
    else:
        ax_clust = fig.add_subplot(gs[0, 0])
        ax_clust.scatter(X_raw[:, 0], X_raw[:, 1], c=colors_pred, alpha=0.5, s=15, edgecolors="none")
        ax_clust.set_xlabel(feature_names[0])
        ax_clust.set_ylabel(feature_names[1])
        ax_clust.set_title("1. Feature Space (Top 2 PC)", fontweight="bold")
        ax_clust.grid(True, alpha=0.3)

    # --- PLOT 2: Feature Importance / Characteristics (Heatmap) ---
    df_feat = pd.DataFrame(X_raw, columns=feature_names)
    df_feat["Regime"] = [state_labels.get(z, f"Regime_{z}") for z in Z_pred]

    # Standardize for Heatmap readability
    df_std = df_feat.iloc[:, :-1].std()
    df_std[df_std == 0] = 1.0
    df_norm = (df_feat.iloc[:, :-1] - df_feat.iloc[:, :-1].mean()) / df_std
    df_norm["Regime"] = df_feat["Regime"]

    regime_means = df_norm.groupby("Regime").mean().T

    # Rename index using descriptions
    new_index = [f"{name}\n{FEATURE_DESCRIPTIONS.get(name, '')}" for name in regime_means.index]
    regime_means.index = new_index

    ax_heat = fig.add_subplot(gs[0, 1])
    try:
        sns.heatmap(
            regime_means,
            annot=True,
            cmap="RdBu_r",
            center=0,
            fmt=".2f",
            ax=ax_heat,
            cbar_kws={"label": "Z-Score Deviation"},
        )
        ax_heat.set_title("2. Regime Characteristics (Feature Means)", fontweight="bold")
        ax_heat.set_yticklabels(ax_heat.get_yticklabels(), rotation=0)
    except Exception:
        logger.exception("Could not plot heatmap")
        ax_heat.text(0.5, 0.5, "Heatmap Error", ha="center")

    # --- PLOT 3: Feature Distributions (Violin Plot of Z-Scores) ---
    ax_viol = fig.add_subplot(gs[0, 2])

    # Ensure palette covers all regimes
    unique_regimes = df_feat["Regime"].unique()
    plot_palette = dict(label_colors)

    for r in unique_regimes:
        if r not in plot_palette:
            h = hash(r)
            plot_palette[r] = cm.tab10(h % 10)

    try:
        df_melt = df_norm.melt(id_vars="Regime", var_name="Feature", value_name="Z-Score")

        sns.violinplot(
            x="Feature", y="Z-Score", hue="Regime", data=df_melt, ax=ax_viol, palette=plot_palette, inner="quartile"
        )

        ax_viol.set_title("3. Feature Distributions (Standardized)", fontweight="bold")
        ax_viol.set_xticklabels(ax_viol.get_xticklabels(), rotation=45, ha="right")
        ax_viol.grid(True, axis="y", alpha=0.3)
        ax_viol.legend(loc="upper right", bbox_to_anchor=(1.0, 1.1), ncol=1, fontsize="small")

    except Exception as e:
        logger.exception("Could not plot violin distribution")
        ax_viol.text(0.5, 0.5, f"Plot Error: {e}", ha="center")

    # --- PLOT 4: Regime Timeline (Overlay) ---
    # User feedback: "Italian flag" (stacked probs) is useless.
    # Reverting to Time Series Overlay which shows the actual market dynamic + regime colors.

    ax_timeline = fig.add_subplot(gs[1, :])

    # Use Price if available, else PC1 (Market Risk)
    if aux_data and "price" in aux_data:
        ts_data = aux_data["price"]
        ts_name = "Price"
    else:
        ts_data = X_raw[:, 0]
        ts_name = f"{feature_names[0]} (Risk Proxy)"

    plot_regime_bg(
        ax_timeline,
        Z_pred,
        ts_data,
        f"4. Market Regime Timeline ({ts_name})",
        ylabel=ts_name,
        state_labels=state_labels,
        label_colors=label_colors,
        dates=aux_data.get("date") if aux_data else None,
    )

    # If we have dates, try to set them (simple approximation if plot_regime_bg plots against index)
    # plot_regime_bg plots ts_data against index. We can set labels manually if needed,
    # but for now simplicity is better.

    # Save the figure
    save_path = os.path.join(os.path.dirname(__file__), "latest_dashboard.png")
    plt.tight_layout()
    plt.savefig(save_path, dpi=100)
    logger.info("Dashboard saved to: %s", save_path)

    # plt.show()


def plot_model_selection(df_results):
    """
    Plot AIC and BIC values
    """
    if df_results.empty or "k" not in df_results.columns:
        logger.info("No model selection results to plot")
        return

    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.plot(df_results["k"], df_results["AIC"], "b-o", label="AIC")
    ax1.set_xlabel("Number of Regimes (k)")
    ax1.set_ylabel("AIC", color="b")
    ax1.tick_params(axis="y", labelcolor="b")
    ax1.grid(True, alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(df_results["k"], df_results["BIC"], "r-s", label="BIC")
    ax2.set_ylabel("BIC", color="r")
    ax2.tick_params(axis="y", labelcolor="r")

    plt.title("Model Selection: AIC vs BIC", fontweight="bold")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper center", bbox_to_anchor=(0.5, 1.1), ncol=2)
    plt.tight_layout()

    save_path = os.path.join(os.path.dirname(__file__), "model_selection.png")
    plt.savefig(save_path, dpi=100)
    logger.info("Model selection plot saved to: %s", save_path)
    # plt.show()
