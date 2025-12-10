
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Patch
from .config import REGIME_NAMES, REGIME_COLORS

def plot_regime_bg(ax, Z, data, title, acc=None, regime_names=None, regime_colors=None, ylabel="Value"):
    """Plot data series with colored regime backgrounds."""
    if regime_names is None:
        regime_names = REGIME_NAMES
    if regime_colors is None:
        regime_colors = REGIME_COLORS
    
    ax.plot(data, 'k', lw=1)
    
    # Find regime change points
    changes = np.concatenate(([0], np.where(Z[:-1] != Z[1:])[0] + 1, [len(Z)]))
    for i in range(len(changes)-1):
        s, e = changes[i], changes[i+1]
        regime_name = regime_names.get(Z[s], f"State {Z[s]}")
        color = regime_colors.get(regime_name, '#95a5a6')
        ax.axvspan(s, e, color=color, alpha=0.3, lw=0)
    
    title_text = title
    if acc is not None:
        title_text += f" (Acc: {acc:.1%})"
    ax.set_title(title_text, fontweight='bold', fontsize=10)
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)

def create_dashboard(X_raw, Z_pred, feature_names, logprob, data_type="Real", Z_true=None):
    """
    Create comprehensive dashboard
    """
    # Use X_raw[:, 0] as the primary time series (Price proxy or Principal Component)
    ts_data = X_raw[:, 0]
    ts_label = feature_names[0]
    
    if data_type == "Real":
        fig = plt.figure(figsize=(18, 10))
        gs = fig.add_gridspec(2, 2)
        
        # --- PLOT 1: Feature Space Clusters (3D if > 2 features, else 2D) ---
        if X_raw.shape[1] >= 3:
            ax_clust = fig.add_subplot(gs[0, 0], projection='3d')
            colors_pred = [REGIME_COLORS.get(REGIME_NAMES.get(z, f"State {z}"), '#95a5a6') for z in Z_pred]
            ax_clust.scatter(X_raw[:, 0], X_raw[:, 1], X_raw[:, 2], c=colors_pred, alpha=0.6, s=20, edgecolors='none')
            ax_clust.set_xlabel(feature_names[0])
            ax_clust.set_ylabel(feature_names[1])
            ax_clust.set_zlabel(feature_names[2])
            ax_clust.set_title("1. 3D Feature Clusters (Colored by Regime)", fontweight='bold')
        else:
            ax_clust = fig.add_subplot(gs[0, 0])
            colors_pred = [REGIME_COLORS.get(REGIME_NAMES.get(z, f"State {z}"), '#95a5a6') for z in Z_pred]
            ax_clust.scatter(X_raw[:, 0], X_raw[:, 1], c=colors_pred, alpha=0.6, s=20, edgecolors='none')
            ax_clust.set_title("1. Feature Space Clusters", fontweight='bold')
            ax_clust.set_xlabel(feature_names[0])
            ax_clust.set_ylabel(feature_names[1])
            ax_clust.grid(True, alpha=0.3)
        
        # --- PLOT 2: Regime Separation (Violin Plot for 1st feature) ---
        ax_dist = fig.add_subplot(gs[0, 1])
        df_plot = pd.DataFrame({'Value': X_raw[:, 0], 'Regime': [REGIME_NAMES.get(z, f"State {z}") for z in Z_pred]})
        
        # Ensure palette covers all regimes (User: this fix is required for k > 3)
        unique_regimes = df_plot['Regime'].unique()
        plot_palette = REGIME_COLORS.copy()
        import matplotlib.cm as cm
        for r in unique_regimes:
            if r not in plot_palette:
                # Generate a consistent color for new regimes
                h = hash(r)
                plot_palette[r] = cm.tab10(h % 10)
                
        sns.violinplot(x='Regime', y='Value', data=df_plot, ax=ax_dist, palette=plot_palette)
        ax_dist.set_title(f"Distribution of {feature_names[0]} by Regime", fontweight='bold')
        
        # --- PLOT 3: Predicted Regimes over Time ---
        ax_pred = fig.add_subplot(gs[1, :])
        plot_regime_bg(ax_pred, Z_pred, ts_data, f"Time Series Regime Overlay ({ts_label})", 
                       ylabel=ts_label)
        
        plt.suptitle(f"HMM Regime Detection (Real Data) | Log-Likelihood: {logprob:.0f}", 
                     fontsize=14, fontweight='bold', y=1.01)
        plt.tight_layout()
        plt.show()

    else:
        # Full Dashboard for Synthetic Data (kept simple as focus is Real)
        fig = plt.figure(figsize=(16, 14))
        gs = fig.add_gridspec(2, 1) # Simplified for brevity in this script
        
        ax_pred = fig.add_subplot(gs[0, :])
        plot_regime_bg(ax_pred, Z_pred, ts_data, "PREDICTED (Viterbi Decoding)", ylabel=ts_label)
        
        plt.suptitle("HMM Regime Detection (Synthetic)", fontsize=14)
        plt.tight_layout()
        plt.show()

def plot_model_selection(df_results):
    """
    Plot AIC and BIC values
    """
    if df_results.empty or 'k' not in df_results.columns:
        print("No model selection results to plot.")
        return

    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.plot(df_results['k'], df_results['AIC'], 'b-o', label='AIC')
    ax1.set_xlabel('Number of Regimes (k)')
    ax1.set_ylabel('AIC', color='b')
    ax1.tick_params(axis='y', labelcolor='b')
    ax1.grid(True, alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(df_results['k'], df_results['BIC'], 'r-s', label='BIC')
    ax2.set_ylabel('BIC', color='r')
    ax2.tick_params(axis='y', labelcolor='r')

    plt.title('Model Selection: AIC vs BIC', fontweight='bold')
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper center", bbox_to_anchor=(0.5, 1.1), ncol=2)
    plt.tight_layout()
    plt.show()
