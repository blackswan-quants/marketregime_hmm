import json
from pathlib import Path

from classes.metrics.standardize import Standardizer
from classes.viz.plotter import Plotter

PROCESSED_DIR = Path("data/processed")
MARKET_FEATURES_PATH = PROCESSED_DIR / "market_features.parquet"
CURVE_CREDIT_PATH = PROCESSED_DIR / "curve_credit_features.parquet"
CROSS_ASSET_PATH = PROCESSED_DIR / "cross_asset_features.parquet"
DATASET_PATH = PROCESSED_DIR / "features_dataset.parquet"
HEATMAP_PATH = PROCESSED_DIR / "corr_heatmap.png"
DIST_PLOT_PATH = PROCESSED_DIR / "feature_distributions.png"
LOSS_REPORT_PATH = PROCESSED_DIR / "feature_loss_report.json"


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
    print(f"Field-loss report saved to {LOSS_REPORT_PATH}")

    print("P2 dataset ready:", DATASET_PATH)
    print("Rows, columns:", features.shape)


if __name__ == "__main__":
    build_features()
