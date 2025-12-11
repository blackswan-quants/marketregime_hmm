
import numpy as np
import os
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
    Create comprehensive dashboard with Feature Interpretability (Heatmap).
    """
    # Use X_raw[:, 0] as the primary time series (Price proxy or Principal Component)
    ts_data = X_raw[:, 0]
    ts_label = feature_names[0]
    
    # Setup Figure
    fig = plt.figure(figsize=(20, 12))
    gs = fig.add_gridspec(2, 3)
    
    title_extra = f" | LogL: {logprob:.0f}" if logprob else ""
    plt.suptitle(f"HMM Regime Detection (Real Data){title_extra}", 
                 fontsize=16, fontweight='bold', y=0.98)

    # --- PLOT 1: Feature Space Clusters (3D Principal Components) ---
    # Always plot first 3 dims if available, else 2
    colors_pred = [REGIME_COLORS.get(REGIME_NAMES.get(z, f"State {z}"), '#95a5a6') for z in Z_pred]
    
    if X_raw.shape[1] >= 3:
        ax_clust = fig.add_subplot(gs[0, 0], projection='3d')
        ax_clust.scatter(X_raw[:, 0], X_raw[:, 1], X_raw[:, 2], c=colors_pred, alpha=0.5, s=15, edgecolors='none')
        ax_clust.set_xlabel(feature_names[0])
        ax_clust.set_ylabel(feature_names[1])
        ax_clust.set_zlabel(feature_names[2])
        ax_clust.set_title("1. Feature Space (Top 3 PC)", fontweight='bold')
    else:
        ax_clust = fig.add_subplot(gs[0, 0])
        ax_clust.scatter(X_raw[:, 0], X_raw[:, 1], c=colors_pred, alpha=0.5, s=15, edgecolors='none')
        ax_clust.set_xlabel(feature_names[0])
        ax_clust.set_ylabel(feature_names[1])
        ax_clust.set_title("1. Feature Space (Top 2 PC)", fontweight='bold')
        ax_clust.grid(True, alpha=0.3)

    # --- PLOT 2: Feature Importance / Characteristics (Heatmap) ---
    # Calculate Mean of each feature per regime
    df_feat = pd.DataFrame(X_raw, columns=feature_names)
    df_feat['Regime'] = [REGIME_NAMES.get(z, f"State {z}") for z in Z_pred]
    
    # Standardize for Heatmap readability (Z-score relative to global mean/std)
    # Avoid division by zero if std is 0
    df_std = df_feat.iloc[:, :-1].std()
    df_std[df_std == 0] = 1.0
    df_norm = (df_feat.iloc[:, :-1] - df_feat.iloc[:, :-1].mean()) / df_std
    df_norm['Regime'] = df_feat['Regime']
    
    regime_means = df_norm.groupby('Regime').mean().T
    
    ax_heat = fig.add_subplot(gs[0, 1])
    try:
        sns.heatmap(regime_means, annot=True, cmap="RdBu_r", center=0, fmt=".2f", ax=ax_heat, cbar_kws={'label': 'Z-Score Deviation'})
        ax_heat.set_title("2. Regime Characteristics (Feature Means)", fontweight='bold')
    except Exception as e:
        print(f"Could not plot heatmap: {e}")
        ax_heat.text(0.5, 0.5, "Heatmap Error", ha='center')
    
    # --- PLOT 3: Distribution of Primary Feature (Violin) ---
    ax_viol = fig.add_subplot(gs[0, 2])
    
    # Ensure palette covers all regimes
    unique_regimes = df_feat['Regime'].unique()
    plot_palette = REGIME_COLORS.copy()
    import matplotlib.cm as cm
    for r in unique_regimes:
        if r not in plot_palette:
            h = hash(r)
            plot_palette[r] = cm.tab10(h % 10)
            
    try:
        sns.violinplot(x='Regime', y=feature_names[0], data=df_feat, ax=ax_viol, palette=plot_palette)
        ax_viol.set_title(f"3. Dist. of {feature_names[0]}", fontweight='bold')
        ax_viol.grid(True, axis='y', alpha=0.3)
    except Exception as e:
        print(f"Could not plot violin: {e}")

    # --- PLOT 4: Predicted Regimes over Time ---
    ax_pred = fig.add_subplot(gs[1, :])
    plot_regime_bg(ax_pred, Z_pred, ts_data, f"4. Time Series Regime Overlay ({ts_label})", 
                   ylabel=ts_label)

    # plt.tight_layout() # Gridspec handles layout better sometimes
    
    # Save the figure
    save_path = os.path.join(os.path.dirname(__file__), "latest_dashboard.png")
    plt.savefig(save_path, dpi=100)
    print(f"Dashboard saved to: {save_path}")
    
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
