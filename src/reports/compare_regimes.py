"""Compare HMM regime predictions with DTW clustering results.

Implements Sprint Point 3: Financial & Statistical Analysis and Interpretation
of HMM vs Clustering Results.

Deliverables:
  (3a) Side-by-side regime overlays on SPX, per-regime stats (mean, vol, drawdown)
  (3b) Financial interpretation validation, alignment metrics (ARI, NMI, confusion matrix)

Outputs saved to data/reports/figures/ and data/reports/.
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import joblib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIGURES_DIR = PROJECT_ROOT / "data" / "reports" / "figures"
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"
HMM_MODEL_PATH = PROJECT_ROOT / "src" / "model" / "hmm_package" / "hmm_model.pkl"
MARKET_FEATURES_PATH = PROJECT_ROOT / "data" / "processed" / "market_features.parquet"
DTW_LABELS_PATH = PROJECT_ROOT / "data" / "processed" / "dtw_regime_labels.csv"
SPX_PATH = PROJECT_ROOT / "data" / "cleaned" / "spx.parquet"

HMM_COLORS = {"Risk-On": "#2ecc71", "Neutral": "#95a5a6", "Risk-Off": "#e74c3c"}
DTW_COLORS = {1: "#2ecc71", 2: "#e74c3c", 3: "#3498db", 4: "#f39c12"}


# ============================================================================
# DATA LOADING
# ============================================================================


def load_hmm_regime_labels() -> pd.DataFrame:
    """Load HMM model and reconstruct dated regime labels.

    Loads the saved model bundle, re-runs the feature pipeline
    (load → smooth → scale → decode) to produce a date-aligned
    regime label series.

    Returns:
        DataFrame with columns [date, hmm_regime] where hmm_regime
        is a string label (e.g. 'Risk-On', 'Risk-Off').

    Raises:
        FileNotFoundError: If model or feature file is missing.
    """
    if not HMM_MODEL_PATH.exists():
        raise FileNotFoundError(f"HMM model not found at {HMM_MODEL_PATH}")
    if not MARKET_FEATURES_PATH.exists():
        raise FileNotFoundError(f"Market features not found at {MARKET_FEATURES_PATH}")

    bundle = joblib.load(HMM_MODEL_PATH)
    model = bundle["model"]
    state_labels = bundle["state_labels"]
    feature_names = bundle["feature_names"]

    df = pd.read_parquet(MARKET_FEATURES_PATH)
    dates = df.index if isinstance(df.index, pd.DatetimeIndex) else pd.to_datetime(df["date"])

    # Replicate the preprocessing from main.py
    features = _extract_features(df, feature_names)
    df_feat = pd.DataFrame(features, columns=feature_names)
    df_smoothed = df_feat.rolling(window=5, min_periods=1).mean()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_smoothed.values)

    _, Z_pred = model.decode(X_scaled)

    regime_labels = [state_labels.get(z, f"Regime_{z}") for z in Z_pred]

    return pd.DataFrame({"date": dates.values[: len(regime_labels)], "hmm_regime": regime_labels})


def _extract_features(df: pd.DataFrame, feature_names: list) -> np.ndarray:
    """Extract feature columns matching the HMM model's expected inputs.

    Mirrors the feature extraction logic in data_loader.load_real_data.

    Args:
        df: Market features DataFrame.
        feature_names: Feature names from the saved model bundle.

    Returns:
        Feature matrix of shape (n_samples, n_features).
    """
    features = []
    for name in feature_names:
        if name == "Log-Returns" and "R1" in df.columns:
            features.append(df["R1"].values)
        elif name == "Log-Volatility" and "V1" in df.columns:
            features.append(np.log(df["V1"].values + 1e-6))
        elif name == "Drawdown" and "D1" in df.columns:
            features.append(df["D1"].values)
        elif name == "Yield Curve" and "M1" in df.columns:
            features.append(df["M1"].values)
        else:
            # Try direct column match
            if name in df.columns:
                features.append(df[name].values)
            else:
                raise ValueError(f"Cannot find feature '{name}' in data columns: {df.columns.tolist()}")
    return np.column_stack(features)


def load_dtw_regime_labels() -> pd.DataFrame:
    """Load DTW clustering regime labels from CSV.

    Returns:
        DataFrame with columns [date, dtw_regime] where dtw_regime
        is an integer cluster ID.

    Raises:
        FileNotFoundError: If DTW labels file is missing.
    """
    if not DTW_LABELS_PATH.exists():
        raise FileNotFoundError(f"DTW labels not found at {DTW_LABELS_PATH}")

    df = pd.read_csv(DTW_LABELS_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.rename(columns={"regime_cluster": "dtw_regime"})
    return df


def load_spx_returns() -> pd.DataFrame:
    """Load SPX prices and compute daily log returns.

    Returns:
        DataFrame with columns [date, close, log_return].
    """
    spx = pd.read_parquet(SPX_PATH)
    if "index" in spx.columns:
        spx = spx.rename(columns={"index": "date"})
    spx["date"] = pd.to_datetime(spx["date"])
    spx = spx.sort_values("date").reset_index(drop=True)
    spx["log_return"] = np.log(spx["close_SPY"] / spx["close_SPY"].shift(1))
    spx = spx.rename(columns={"close_SPY": "close"})
    return spx[["date", "close", "log_return"]].dropna()


def merge_all_labels() -> pd.DataFrame:
    """Load and merge HMM labels, DTW labels, and SPX returns on date.

    Returns:
        DataFrame with columns [date, close, log_return, hmm_regime, dtw_regime]
        restricted to the date intersection of all three sources.
    """
    hmm_labels = load_hmm_regime_labels()
    dtw_labels = load_dtw_regime_labels()
    spx = load_spx_returns()

    merged = spx.merge(hmm_labels, on="date", how="inner")
    merged = merged.merge(dtw_labels, on="date", how="inner")
    merged = merged.sort_values("date").reset_index(drop=True)

    logger.info(
        "Merged %d overlapping dates (HMM=%d, DTW=%d, SPX=%d)",
        len(merged),
        len(hmm_labels),
        len(dtw_labels),
        len(spx),
    )
    return merged


# ============================================================================
# AGREEMENT METRICS (3b)
# ============================================================================


def compute_agreement_metrics(merged: pd.DataFrame) -> Dict[str, float]:
    """Compute clustering agreement metrics between HMM and DTW regimes.

    Args:
        merged: DataFrame with hmm_regime and dtw_regime columns.

    Returns:
        Dict with adjusted_rand_index, normalized_mutual_info, and
        per-pair agreement_rate.
    """
    hmm = merged["hmm_regime"]
    dtw = merged["dtw_regime"]

    ari = adjusted_rand_score(hmm, dtw)
    nmi = normalized_mutual_info_score(hmm, dtw)

    metrics = {
        "adjusted_rand_index": ari,
        "normalized_mutual_info": nmi,
    }

    logger.info("Agreement: ARI=%.4f, NMI=%.4f", ari, nmi)
    return metrics


def compute_confusion_matrix(merged: pd.DataFrame) -> pd.DataFrame:
    """Build a cross-tabulation of HMM regimes vs DTW clusters.

    Uses pandas crosstab instead of sklearn confusion_matrix because
    HMM labels are strings and DTW labels are integers.

    Args:
        merged: DataFrame with hmm_regime and dtw_regime columns.

    Returns:
        DataFrame with HMM regimes as rows and DTW clusters as columns.
    """
    ct = pd.crosstab(
        merged["hmm_regime"],
        merged["dtw_regime"],
        rownames=["HMM Regime"],
        colnames=["DTW Cluster"],
    )
    ct.columns = [f"DTW_{c}" for c in ct.columns]
    return ct


# ============================================================================
# PER-REGIME STATISTICS (3a)
# ============================================================================


def _max_drawdown(returns: pd.Series) -> float:
    """Compute maximum drawdown from a series of log returns.

    Args:
        returns: Series of daily log returns.

    Returns:
        Maximum drawdown as a negative float (e.g. -0.15 for 15% drawdown).
    """
    cumulative = (1 + returns).cumprod()
    peak = cumulative.cummax()
    drawdown = (cumulative - peak) / peak
    return float(drawdown.min())


def compute_per_regime_stats(
    merged: pd.DataFrame,
    regime_col: str,
) -> pd.DataFrame:
    """Compute per-regime financial statistics.

    Computes mean daily return, annualized volatility, max drawdown,
    observation count, and win rate for each regime.

    Args:
        merged: DataFrame with log_return and regime columns.
        regime_col: Name of the regime column ('hmm_regime' or 'dtw_regime').

    Returns:
        DataFrame indexed by regime with statistic columns.
    """
    rows = []
    for regime, group in merged.groupby(regime_col):
        rets = group["log_return"]
        rows.append(
            {
                "regime": regime,
                "mean_daily_return_pct": float(rets.mean() * 100),
                "annualized_vol_pct": float(rets.std() * np.sqrt(252) * 100),
                "max_drawdown_pct": float(_max_drawdown(rets) * 100),
                "obs_count": len(rets),
                "win_rate_pct": float((rets > 0).mean() * 100),
            }
        )

    return pd.DataFrame(rows).set_index("regime")


# ============================================================================
# FINANCIAL INTERPRETATION VALIDATION (3b)
# ============================================================================


def validate_financial_interpretation(
    stats: pd.DataFrame,
    method_name: str,
) -> pd.DataFrame:
    """Validate that detected regimes match expected financial behavior.

    Checks whether the lowest-volatility regime has the highest mean return
    (bull) and whether the highest-volatility regime has the lowest mean return
    (bear).

    Args:
        stats: Per-regime stats from compute_per_regime_stats.
        method_name: 'HMM' or 'DTW' for labeling.

    Returns:
        DataFrame with regime, role (bull/bear), and pass/fail for each check.
    """
    sorted_by_vol = stats.sort_values("annualized_vol_pct")

    bull_regime = sorted_by_vol.index[0]
    bear_regime = sorted_by_vol.index[-1]

    bull_stats = sorted_by_vol.loc[bull_regime]
    bear_stats = sorted_by_vol.loc[bear_regime]

    checks = [
        {
            "method": method_name,
            "regime": bull_regime,
            "role": "Bull",
            "check": "positive mean return",
            "value": f"{bull_stats['mean_daily_return_pct']:.4f}%",
            "pass": bull_stats["mean_daily_return_pct"] > 0,
        },
        {
            "method": method_name,
            "regime": bull_regime,
            "role": "Bull",
            "check": "lowest volatility",
            "value": f"{bull_stats['annualized_vol_pct']:.2f}%",
            "pass": True,  # By construction (sorted)
        },
        {
            "method": method_name,
            "regime": bear_regime,
            "role": "Bear",
            "check": "negative mean return",
            "value": f"{bear_stats['mean_daily_return_pct']:.4f}%",
            "pass": bear_stats["mean_daily_return_pct"] < 0,
        },
        {
            "method": method_name,
            "regime": bear_regime,
            "role": "Bear",
            "check": "highest volatility",
            "value": f"{bear_stats['annualized_vol_pct']:.2f}%",
            "pass": True,  # By construction (sorted)
        },
    ]

    return pd.DataFrame(checks)


# ============================================================================
# VISUALIZATIONS (3a + 3b)
# ============================================================================


def _shade_regimes(
    ax: plt.Axes,
    dates: np.ndarray,
    regimes: np.ndarray,
    color_map: dict,
) -> None:
    """Add colored background spans for regime periods.

    Args:
        ax: Matplotlib axes to draw on.
        dates: Array of datetime values.
        regimes: Array of regime labels (same length as dates).
        color_map: Mapping from regime label to hex color string.
    """
    current = None
    start = None
    for i, (d, r) in enumerate(zip(dates, regimes)):
        if r != current:
            if current is not None:
                color = color_map.get(current, "#95a5a6")
                ax.axvspan(start, d, alpha=0.25, color=color, zorder=1)
            current = r
            start = d
    if current is not None:
        color = color_map.get(current, "#95a5a6")
        ax.axvspan(start, dates[-1], alpha=0.25, color=color, zorder=1)


def plot_side_by_side_regimes(
    merged: pd.DataFrame,
    output_path: Optional[Path] = None,
) -> None:
    """Plot HMM and DTW regimes overlaid on SPX price in a two-panel figure.

    Args:
        merged: Merged DataFrame from merge_all_labels.
        output_path: Path to save figure. Defaults to FIGURES_DIR/hmm_vs_dtw_regimes.png.
    """
    output_path = output_path or (FIGURES_DIR / "hmm_vs_dtw_regimes.png")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dates = pd.to_datetime(merged["date"]).values
    price = merged["close"].values

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True)

    # Panel 1: HMM regimes
    _shade_regimes(ax1, dates, merged["hmm_regime"].values, HMM_COLORS)
    ax1.plot(dates, price, color="black", linewidth=1.5, zorder=10)
    ax1.set_ylabel("SPY Close", fontsize=12)
    ax1.set_title("HMM Regime Detection", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.3)

    # Legend for HMM
    for label, color in HMM_COLORS.items():
        if label in merged["hmm_regime"].values:
            ax1.axvspan(dates[0], dates[0], alpha=0.25, color=color, label=label)
    ax1.legend(loc="upper left", fontsize=10)

    # Panel 2: DTW regimes
    dtw_labels = merged["dtw_regime"].values
    _shade_regimes(ax2, dates, dtw_labels, DTW_COLORS)
    ax2.plot(dates, price, color="black", linewidth=1.5, zorder=10)
    ax2.set_ylabel("SPY Close", fontsize=12)
    ax2.set_title("DTW Hierarchical Clustering", fontsize=14, fontweight="bold")
    ax2.grid(True, alpha=0.3)
    ax2.set_xlabel("Date", fontsize=12)

    # Legend for DTW
    for cid in sorted(merged["dtw_regime"].unique()):
        color = DTW_COLORS.get(int(cid), "#95a5a6")
        ax2.axvspan(dates[0], dates[0], alpha=0.25, color=color, label=f"Cluster {cid}")
    ax2.legend(loc="upper left", fontsize=10)

    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="right")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close()
    logger.info("Side-by-side regime plot saved to %s", output_path)


def plot_regime_statistics_comparison(
    hmm_stats: pd.DataFrame,
    dtw_stats: pd.DataFrame,
    output_path: Optional[Path] = None,
) -> None:
    """Plot per-regime statistics (return, vol, drawdown) for both methods.

    Creates a 2x3 bar chart grid: top row HMM, bottom row DTW, columns are
    mean return, annualized volatility, and max drawdown.

    Args:
        hmm_stats: Per-regime stats for HMM from compute_per_regime_stats.
        dtw_stats: Per-regime stats for DTW from compute_per_regime_stats.
        output_path: Path to save figure.
    """
    output_path = output_path or (FIGURES_DIR / "regime_stats_comparison.png")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 3, figsize=(18, 10))

    stat_cols = [
        ("mean_daily_return_pct", "Mean Daily Return (%)", "0"),
        ("annualized_vol_pct", "Annualized Volatility (%)", None),
        ("max_drawdown_pct", "Max Drawdown (%)", None),
    ]

    for row, (stats, method, color_map) in enumerate([(hmm_stats, "HMM", HMM_COLORS), (dtw_stats, "DTW", DTW_COLORS)]):
        for col, (stat_key, ylabel, hline) in enumerate(stat_cols):
            ax = axes[row, col]
            regimes = stats.index.tolist()
            values = stats[stat_key].values
            colors = [color_map.get(r, "#95a5a6") for r in regimes]

            ax.bar(
                [str(r) for r in regimes],
                values,
                color=colors,
                alpha=0.7,
                edgecolor="black",
            )
            ax.set_ylabel(ylabel, fontsize=11)
            ax.set_title(f"{method}: {ylabel}", fontsize=12, fontweight="bold")
            ax.grid(True, alpha=0.3, axis="y")

            if hline is not None:
                ax.axhline(y=float(hline), color="black", linestyle="--", linewidth=0.8)

            # Value labels on bars
            for i, v in enumerate(values):
                offset = abs(v) * 0.05 + 0.01
                y_pos = v + offset if v >= 0 else v - offset
                ax.text(i, y_pos, f"{v:.3f}", ha="center", fontsize=9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close()
    logger.info("Statistics comparison plot saved to %s", output_path)


def plot_confusion_heatmap(
    cm_df: pd.DataFrame,
    output_path: Optional[Path] = None,
) -> None:
    """Plot confusion matrix between HMM and DTW regimes as a heatmap.

    Args:
        cm_df: Confusion matrix DataFrame from compute_confusion_matrix.
        output_path: Path to save figure.
    """
    output_path = output_path or (FIGURES_DIR / "hmm_dtw_confusion_matrix.png")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(8, 6))

    im = ax.imshow(cm_df.values, cmap="Blues", aspect="auto")
    fig.colorbar(im, ax=ax, label="Count")

    ax.set_xticks(range(len(cm_df.columns)))
    ax.set_xticklabels(cm_df.columns, fontsize=11)
    ax.set_yticks(range(len(cm_df.index)))
    ax.set_yticklabels(cm_df.index, fontsize=11)
    ax.set_xlabel("DTW Cluster", fontsize=12)
    ax.set_ylabel("HMM Regime", fontsize=12)
    ax.set_title("HMM vs DTW: Confusion Matrix", fontsize=14, fontweight="bold")

    # Annotate cells
    for i in range(len(cm_df.index)):
        for j in range(len(cm_df.columns)):
            val = cm_df.values[i, j]
            text_color = "white" if val > cm_df.values.max() / 2 else "black"
            ax.text(j, i, str(val), ha="center", va="center", fontsize=14, color=text_color)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.savefig(output_path.with_suffix(".svg"), bbox_inches="tight")
    plt.close()
    logger.info("Confusion matrix saved to %s", output_path)


# ============================================================================
# REPORT GENERATION
# ============================================================================


def generate_comparison_report(
    merged: pd.DataFrame,
    hmm_stats: pd.DataFrame,
    dtw_stats: pd.DataFrame,
    agreement: Dict[str, float],
    cm_df: pd.DataFrame,
    hmm_validation: pd.DataFrame,
    dtw_validation: pd.DataFrame,
    output_path: Optional[Path] = None,
) -> None:
    """Generate a markdown report summarizing HMM vs DTW comparison.

    Args:
        merged: Merged regime DataFrame.
        hmm_stats: HMM per-regime statistics.
        dtw_stats: DTW per-regime statistics.
        agreement: Agreement metrics dict.
        cm_df: Confusion matrix DataFrame.
        hmm_validation: HMM financial validation DataFrame.
        dtw_validation: DTW financial validation DataFrame.
        output_path: Path to save report.
    """
    output_path = output_path or (REPORTS_DIR / "hmm_vs_dtw_comparison.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    date_min = pd.Timestamp(merged["date"].min()).strftime("%Y-%m-%d")
    date_max = pd.Timestamp(merged["date"].max()).strftime("%Y-%m-%d")
    generated = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    ari_val = agreement["adjusted_rand_index"]
    nmi_val = agreement["normalized_mutual_info"]
    ari_interp = _interpret_ari(ari_val)
    nmi_interp = _interpret_nmi(nmi_val)

    report = f"""# HMM vs DTW Clustering: Regime Comparison Report

**Generated:** {generated}
**Date Range:** {date_min} to {date_max}
**Overlapping Observations:** {len(merged)}

---

## 1. Agreement Metrics

| Metric | Value | Interpretation |
|--------|-------|----------------|
| Adjusted Rand Index | {ari_val:.4f} | {ari_interp} |
| Normalized Mutual Info | {nmi_val:.4f} | {nmi_interp} |

---

## 2. Confusion Matrix (HMM rows x DTW columns)

{_df_to_md_table(cm_df, show_index=True)}

---

## 3. Per-Regime Statistics

### HMM Regimes

{_df_to_md_table(hmm_stats, show_index=True)}

### DTW Clusters

{_df_to_md_table(dtw_stats, show_index=True)}

---

## 4. Financial Interpretation Validation

### HMM

{_df_to_md_table(hmm_validation, show_index=False)}

### DTW

{_df_to_md_table(dtw_validation, show_index=False)}

---

## 5. Figures

- `hmm_vs_dtw_regimes.png` — Side-by-side regime timelines on SPX price
- `regime_stats_comparison.png` — Per-regime return, volatility, drawdown bars
- `hmm_dtw_confusion_matrix.png` — Confusion matrix heatmap
"""

    with open(output_path, "w") as f:
        f.write(report)
    logger.info("Comparison report saved to %s", output_path)


def _df_to_md_table(df: pd.DataFrame, show_index: bool = True) -> str:
    """Convert a DataFrame to a markdown table string without tabulate.

    Args:
        df: DataFrame to convert.
        show_index: Whether to include the index as the first column.

    Returns:
        Markdown-formatted table string.
    """
    cols = list(df.columns)
    if show_index:
        header = [str(df.index.name or "")] + [str(c) for c in cols]
        rows = [[str(idx)] + [str(df.at[idx, c]) for c in cols] for idx in df.index]
    else:
        header = [str(c) for c in cols]
        rows = [[str(row[c]) for c in cols] for _, row in df.iterrows()]

    lines = [" | ".join(header), " | ".join(["---"] * len(header))]
    for row in rows:
        lines.append(" | ".join(row))
    return "\n".join(lines)


def _interpret_ari(ari: float) -> str:
    """Return a short interpretation string for ARI value.

    Args:
        ari: Adjusted Rand Index value.

    Returns:
        Human-readable interpretation.
    """
    if ari > 0.65:
        return "Strong agreement"
    if ari > 0.25:
        return "Moderate agreement"
    return "Weak agreement"


def _interpret_nmi(nmi: float) -> str:
    """Return a short interpretation string for NMI value.

    Args:
        nmi: Normalized Mutual Information value.

    Returns:
        Human-readable interpretation.
    """
    if nmi > 0.65:
        return "High shared information"
    if nmi > 0.25:
        return "Moderate shared information"
    return "Low shared information"


# ============================================================================
# MAIN PIPELINE
# ============================================================================


def run_comparison() -> None:
    """Run the full HMM vs DTW comparison pipeline.

    Loads both regime label sets, merges with SPX returns, computes
    agreement metrics, per-regime statistics, financial validation,
    and generates all plots and the summary report.
    """
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Load and merge
    logger.info("Loading and merging regime labels with SPX returns")
    merged = merge_all_labels()

    if merged.empty:
        logger.error("No overlapping dates between HMM, DTW, and SPX — cannot compare")
        return

    # Agreement metrics
    logger.info("Computing agreement metrics")
    agreement = compute_agreement_metrics(merged)
    cm_df = compute_confusion_matrix(merged)

    # Per-regime statistics
    logger.info("Computing per-regime statistics")
    hmm_stats = compute_per_regime_stats(merged, "hmm_regime")
    dtw_stats = compute_per_regime_stats(merged, "dtw_regime")

    # Financial validation
    logger.info("Validating financial interpretation")
    hmm_validation = validate_financial_interpretation(hmm_stats, "HMM")
    dtw_validation = validate_financial_interpretation(dtw_stats, "DTW")

    # Plots
    logger.info("Generating plots")
    plot_side_by_side_regimes(merged)
    plot_regime_statistics_comparison(hmm_stats, dtw_stats)
    plot_confusion_heatmap(cm_df)

    # Report
    generate_comparison_report(merged, hmm_stats, dtw_stats, agreement, cm_df, hmm_validation, dtw_validation)

    # Console summary
    logger.info("=" * 60)
    logger.info("COMPARISON COMPLETE")
    logger.info("=" * 60)
    logger.info("ARI=%.4f, NMI=%.4f", agreement["adjusted_rand_index"], agreement["normalized_mutual_info"])
    logger.info("HMM regime stats:\n%s", hmm_stats.to_string())
    logger.info("DTW regime stats:\n%s", dtw_stats.to_string())
    logger.info(
        "Financial validation: HMM %d/%d pass, DTW %d/%d pass",
        hmm_validation["pass"].sum(),
        len(hmm_validation),
        dtw_validation["pass"].sum(),
        len(dtw_validation),
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run_comparison()
