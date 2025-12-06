import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # noqa: E402
sys.path.insert(0, str(PROJECT_ROOT / "src"))  # noqa: E402

from classes.metrics.preprocessing import compute_distribution_stats, compute_vif, reduce_collinearity_via_vif, run_pca
from classes.metrics.standardize import Standardizer
from classes.viz.plotter import Plotter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")
REPORTS_DIR = Path("data/reports")
MARKET_FEATURES_PATH = PROCESSED_DIR / "market_features.parquet"
CURVE_CREDIT_PATH = PROCESSED_DIR / "curve_credit_features.parquet"
CROSS_ASSET_PATH = PROCESSED_DIR / "cross_asset_features.parquet"
DATASET_PATH = PROCESSED_DIR / "features_dataset.parquet"
HEATMAP_PATH = REPORTS_DIR / "figures" / "corr_heatmap.png"
DIST_PLOT_PATH = REPORTS_DIR / "figures" / "feature_distributions.png"
LOSS_REPORT_PATH = REPORTS_DIR / "feature_loss_report.json"

# Analysis Outputs
DIST_STATS_PATH = PROCESSED_DIR / "distribution" / "distribution_stats.csv"
VIF_PATH = PROCESSED_DIR / "distribution" / "vif_stats.csv"
HMM_INPUT_DIR = PROCESSED_DIR / "hmm_input"
HMM_INPUT_PATH = HMM_INPUT_DIR / "hmm_model_input.parquet"
HMM_INPUT_CSV_PATH = HMM_INPUT_DIR / "hmm_model_input.csv"
SCREE_DIR = REPORTS_DIR / "scree"


def build_features() -> None:
    """Execute the full feature build pipeline end-to-end."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    SCREE_DIR.mkdir(parents=True, exist_ok=True)

    features, alignment_report = Standardizer.load_and_align(
        [
            str(MARKET_FEATURES_PATH),
            str(CURVE_CREDIT_PATH),
            str(CROSS_ASSET_PATH),
        ],
        return_report=True,
    )
    features = Standardizer.handle_na(features)
    aligned_rows = alignment_report["aligned_rows"]
    final_rows = len(features)
    na_rows_dropped = max(aligned_rows - final_rows, 0)

    standardizer = Standardizer()
    features = standardizer.fit_transform(features)

    features.to_parquet(DATASET_PATH, index=False)
    z_cols = [col for col in features.columns if col.endswith("_z")]

    if z_cols:
        plotter = Plotter()
        plotter.correlation_heatmap(
            features[z_cols],
            output_path=str(HEATMAP_PATH),
            exclude_cols=[],
            figsize=(14, 12),
            annot=True,
            mask_upper=False,
            title="Feature Correlation Heatmap",
        )

        plotter.feature_distributions(
            features[z_cols],
            output_path=str(DIST_PLOT_PATH),
            exclude_cols=[],
            n_cols=4,
        )

        logger.info("Starting Feature Analysis (Distributions, VIF, PCA)...")

        # Distribution Stats
        dist_stats = compute_distribution_stats(features[z_cols])
        DIST_STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
        dist_stats.to_csv(DIST_STATS_PATH, index=False)
        logger.info(f"Distribution stats saved to {DIST_STATS_PATH}")

        # VIF
        vif_stats = compute_vif(features[z_cols])
        vif_stats.to_csv(VIF_PATH, index=False)
        logger.info(f"VIF stats saved to {VIF_PATH}")

        # Check for high VIF
        high_vif = vif_stats[vif_stats["high_collinearity"]]
        if not high_vif.empty:
            logger.warning(f"Features with high VIF (>5):\n{high_vif}")
        # Check for candidates to drop based on VIF > 5 (Iterative Method)
        _, dropped_vars = reduce_collinearity_via_vif(features[z_cols], threshold=5.0)
        if dropped_vars:
            logger.warning(f"Iterative VIF removal (threshold=5) suggests dropping: {dropped_vars}")
            logger.info("Proceeding with all features since PCA will be used to combine them")
        else:
            logger.info("No features need removal based on VIF > 5")

        # PCA per Category
        # Define categories based on naming conventions
        # Market: Start with R, M, V, S, D
        # Credit: curve_*, credit_*
        # Cross: X*, M_LV*, M_CH*
        all_cols = z_cols
        credit_cols = [c for c in all_cols if c.startswith("curve_") or c.startswith("credit_")]
        cross_cols = [c for c in all_cols if c.startswith("X") or c.startswith("M_LV") or c.startswith("M_CH")]
        market_cols = [c for c in all_cols if c not in credit_cols and c not in cross_cols]

        categories = {"market": market_cols, "credit": credit_cols, "cross_asset": cross_cols}

        hmm_input_df = pd.DataFrame(index=features.index)
        # Include date
        if "date" in features.columns:
            hmm_input_df["date"] = features["date"]

        for cat_name, col_list in categories.items():
            if not col_list:
                logger.warning(f"No columns found for category {cat_name}, skipping PCA.")
                continue

            logger.info(f"Running PCA for {cat_name} with {len(col_list)} features...")

            # Run PCA (retain 80% variance)
            variance_threshold = 0.80
            try:
                # n_components=None means keep all
                pca, scores_full, loadings_full = run_pca(features, n_components=None, columns_to_include=col_list)

                # eigenvalues = pca.explained_variance_
                explained_var_ratio = pca.explained_variance_ratio_
                cum_var = np.cumsum(explained_var_ratio)

                # Kaiser Criterion (Eigenvalue > 1.0) - If we want to use this
                # n_kaiser = np.sum(eigenvalues > 1.0)

                # Variance Threshold (80% strict)
                n_var80 = np.argmax(cum_var >= variance_threshold) + 1

                # Select N based on user request: 80% Variance
                n_selected = n_var80

                logger.info(f"PCA Verification for {cat_name}:")
                # logger.info(f" - Kaiser (Eig > 1): {n_kaiser} components")
                logger.info(f" - 80% Variance: {n_var80} components")
                logger.info(f" -> Selected n={n_selected} components (Explains {cum_var[n_selected-1]:.2%})")

                # Slice the results
                scores_selected = scores_full.iloc[
                    :, :n_selected
                ]  # Select only the first n_selected columns (Principal Components)
                # explained_var_selected = explained_var_ratio[:n_selected] # Select variance ratio for these top components only  # noqa: E501

                scores_selected.columns = [f"{cat_name}_{c}" for c in scores_selected.columns]
                hmm_input_df = pd.concat([hmm_input_df, scores_selected], axis=1)

                # Plot
                plotter.plot_pca_components(
                    explained_var_ratio, scores_selected, output_path_scree=str(SCREE_DIR / f"{cat_name}_scree.png")
                )
                logger.info(f"PCA for {cat_name} completed. Retained {n_selected} components.")

            except Exception as e:
                logger.error(f"PCA failed for {cat_name}: {e}")

        # Save HMM Input
        logger.info(f"Columns in HMM Input before saving: {hmm_input_df.columns.tolist()}")
        logger.info(f"First 5 rows of HMM Input:\n{hmm_input_df.head()}")

        HMM_INPUT_DIR.mkdir(parents=True, exist_ok=True)
        hmm_input_df.to_parquet(HMM_INPUT_PATH, index=False)
        hmm_input_df.to_csv(HMM_INPUT_CSV_PATH, index=False)
        logger.info(f"HMM Model Input saved to {HMM_INPUT_PATH}")
        logger.info(f"Final HMM Input Shape: {hmm_input_df.shape}")

    loss_report = {
        "alignment_method": alignment_report["alignment_method"],
        "source_rows": alignment_report["source_rows"],
        "source_date_ranges": alignment_report["source_date_ranges"],
        "rows_dropped_by_source": alignment_report["rows_dropped_by_source"],
        "aligned_rows": aligned_rows,
        "aligned_date_range": alignment_report["aligned_date_range"],
        "final_rows": final_rows,
        "na_rows_dropped": na_rows_dropped,
        "total_rows_lost": sum(alignment_report["rows_dropped_by_source"].values()) + na_rows_dropped,
    }
    LOSS_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOSS_REPORT_PATH, "w", encoding="utf-8") as handle:
        json.dump(loss_report, handle, indent=2)
    logger.info(f"Field-loss report saved to {LOSS_REPORT_PATH}")

    logger.info(f"P2 dataset ready: {DATASET_PATH}")
    logger.info(f"Rows, columns: {features.shape}")


if __name__ == "__main__":
    build_features()
