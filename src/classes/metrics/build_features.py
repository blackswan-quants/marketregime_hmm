import json
import logging
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # noqa: E402
sys.path.insert(0, str(PROJECT_ROOT / "src"))  # noqa: E402

from classes.metrics.standardize import Standardizer
from classes.viz.plotter import Plotter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")
MARKET_FEATURES_PATH = PROCESSED_DIR / "market_features.parquet"
CURVE_CREDIT_PATH = PROCESSED_DIR / "curve_credit_features.parquet"
CROSS_ASSET_PATH = PROCESSED_DIR / "cross_asset_features.parquet"
DATASET_PATH = PROCESSED_DIR / "features_dataset.parquet"
HEATMAP_PATH = PROCESSED_DIR / "corr_heatmap.png"
DIST_PLOT_PATH = PROCESSED_DIR / "feature_distributions.png"
LOSS_REPORT_PATH = PROCESSED_DIR / "feature_loss_report.json"
CORRELATION_GROUPS_PATH = PROCESSED_DIR / "correlation_groups.json"


def find_correlation_groups(
    df: pd.DataFrame, threshold: float = 0.8, exclude_cols: List[str] = None
) -> Dict[str, List[str]]:
    """Find groups of features where all pairs have correlation >= threshold.

    Args:
        df: DataFrame with features to analyze
        threshold: Minimum correlation threshold (default 0.8)
        exclude_cols: Columns to exclude from analysis

    Returns:
        Dictionary mapping group names to lists of feature names
    """
    if exclude_cols is None:
        exclude_cols = []

    cols = [col for col in df.columns if col not in exclude_cols]
    corr_matrix = df[cols].corr()

    # Track which features have been assigned to groups
    assigned = set()
    groups = []

    # For each feature, try to build a maximal clique starting from it
    for start_idx in range(len(cols)):
        start_feat = cols[start_idx]

        if start_feat in assigned:
            continue

        # Start a new potential group
        candidate_group = {start_feat}

        # Try to add more features
        for candidate_idx in range(len(cols)):
            candidate_feat = cols[candidate_idx]

            if candidate_feat == start_feat or candidate_feat in assigned:
                continue

            # Check if candidate is highly correlated with all features in current group
            is_compatible = True
            for group_feat in candidate_group:
                feat_idx = cols.index(group_feat)
                corr_val = corr_matrix.iloc[candidate_idx, feat_idx]
                if corr_val < threshold:
                    is_compatible = False
                    break

            if is_compatible:
                candidate_group.add(candidate_feat)

        # Only keep groups with at least 2 features
        if len(candidate_group) >= 2:
            # Check if this is a new group (not a subset of existing groups)
            is_new = True
            for existing_group in groups:
                if candidate_group.issubset(existing_group):
                    is_new = False
                    break
                elif existing_group.issubset(candidate_group):
                    groups.remove(existing_group)
                    for feat in existing_group:
                        assigned.discard(feat)

            if is_new:
                groups.append(candidate_group)
                assigned.update(candidate_group)

    # Convert to dictionary
    result = {}
    for idx, group in enumerate(sorted(groups, key=len, reverse=True)):
        sorted_features = sorted(list(group))
        group_name = f"Group_{idx + 1}"
        result[group_name] = sorted_features

    return result


def print_correlation_groups(groups: Dict[str, List[str]], df: pd.DataFrame) -> None:
    """Log correlation groups with pairwise correlations.

    Args:
        groups: Dictionary of correlation groups
        df: DataFrame with features to compute correlations
    """

    if not groups:
        logger.info("No groups found where all pairs have correlation >= 0.8")
        return

    for group_name, features in groups.items():
        logger.info(f" {group_name}: {len(features)} features")
        logger.info("")
        for feat in features:
            logger.info(f"  - {feat}")

        if len(features) > 1:
            logger.info(" Pairwise correlations:")
            corr_sub = df[features].corr()

            for i in range(len(features)):
                for j in range(i + 1, len(features)):
                    corr_val = corr_sub.iloc[i, j]
                    logger.info(f"    {features[i]} <-> {features[j]}: {corr_val:.3f}")

        logger.info("")
        logger.info("-" * 60)
        logger.info("")


def build_features() -> None:
    """Execute the full feature build pipeline end-to-end."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

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

        # Find correlation groups
        correlation_groups = find_correlation_groups(features[z_cols], threshold=0.8, exclude_cols=["date"])
        logger.info("")
        print_correlation_groups(correlation_groups, features[z_cols])

        with open(CORRELATION_GROUPS_PATH, "w", encoding="utf-8") as f:
            json.dump(correlation_groups, f, indent=2)
        logger.info(f"Correlation groups saved to {CORRELATION_GROUPS_PATH}")

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
