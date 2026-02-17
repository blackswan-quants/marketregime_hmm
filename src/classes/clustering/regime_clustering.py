"""Regime detection via hierarchical DTW clustering.

Subclasses TimeSeriesClustering from BSQ helpermodules with three bug fixes:
  1. Use dtaidistance C implementation instead of per-pair Python fastdtw
  2. Avoid granular joblib task creation (dtaidistance handles parallelism)
  3. Pass condensed distance vector to linkage() instead of NxN matrix

Pipeline:
  - Loads PCA-reduced features from data/processed/hmm_input/
  - Segments into rolling 21-day windows
  - Computes multivariate DTW distance matrix
  - Applies hierarchical clustering (2-4 clusters)
  - Maps labels back to timeline and overlays on SPX price chart
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dtaidistance import dtw_ndim
from scipy.cluster.hierarchy import dendrogram, fcluster, linkage
from scipy.spatial.distance import squareform
from sklearn.metrics import silhouette_score

PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Make helpermodules importable
sys.path.insert(0, str(PROJECT_ROOT / "helpermodules"))
from clustering_class import TimeSeriesClustering  # noqa: E402

logger = logging.getLogger(__name__)

HMM_INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "hmm_input" / "hmm_model_input.parquet"
SPX_PATH = PROJECT_ROOT / "data" / "cleaned" / "spx.parquet"
FIGURES_DIR = PROJECT_ROOT / "data" / "reports" / "figures"
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

REGIME_COLORS = {
    1: "#2ecc71",  # Green
    2: "#e74c3c",  # Red
    3: "#3498db",  # Blue
    4: "#f39c12",  # Orange
}


class RegimeClustering(TimeSeriesClustering):
    """Hierarchical DTW clustering on rolling windows of multivariate features.

    Extends TimeSeriesClustering from helpermodules with:
      - Rolling window construction for regime detection
      - Multivariate DTW via dtaidistance C implementation (bug fix #1 & #2)
      - Correct condensed distance vector for linkage() (bug fix #3)
      - Timeline mapping and SPX overlay plotting

    Args:
        df: DataFrame with a ``date`` column and numeric feature columns.
        window_size: Rolling window length in trading days (default 21).
    """

    def __init__(self, df: pd.DataFrame, window_size: int = 21):
        self.window_size = window_size

        # Separate date from features
        if "date" not in df.columns:
            raise ValueError("DataFrame must contain a 'date' column")

        self.dates = pd.to_datetime(df["date"]).values
        self.feature_names = [c for c in df.columns if c != "date"]
        feature_data = df[self.feature_names].astype(np.double)

        # Initialize parent class with features-only DataFrame (rows=observations, cols=features)
        # Parent validates: numeric, no NaN, no inf
        super().__init__(feature_data)

        self.n_features = feature_data.shape[1]
        logger.info(
            "RegimeClustering: %d rows, %d features, window_size=%d",
            feature_data.shape[0],
            self.n_features,
            self.window_size,
        )

        self.windows: Optional[np.ndarray] = None
        self.window_dates: Optional[np.ndarray] = None
        self.condensed_dist: Optional[np.ndarray] = None

    def create_rolling_windows(self) -> np.ndarray:
        """Segment features into rolling windows.

        Returns:
            3D array of shape (n_windows, window_size, n_features).
        """
        data = self.raw_data.values
        T = data.shape[0]
        n_windows = T - self.window_size + 1
        if n_windows < 2:
            raise ValueError(
                f"Not enough data for windowing: {T} rows with window_size={self.window_size} "
                f"yields {n_windows} windows (need >= 2)"
            )

        self.windows = np.array(
            [data[i : i + self.window_size] for i in range(n_windows)],
            dtype=np.double,
        )
        # Each window is labeled by its LAST date (avoids look-ahead bias)
        self.window_dates = self.dates[self.window_size - 1 :]

        logger.info(
            "Created %d rolling windows of shape (%d, %d)",
            n_windows,
            self.window_size,
            self.n_features,
        )
        return self.windows

    def _compute_distance_matrix(self, metric: str = "dtw", **dtw_args) -> np.ndarray:
        """Compute multivariate DTW distance matrix using dtaidistance C implementation.

        Overrides the parent method to fix bugs #1 and #2:
          - Bug #1: Parent creates one joblib task per pair (~13k tasks). The dtaidistance
            C implementation handles parallelism internally via OpenMP.
          - Bug #2: Parent uses Python fastdtw library. dtaidistance is ~50x faster.

        Args:
            metric: Distance metric. Only 'dtw' is supported for multivariate windows.
            **dtw_args: Passed to dtw_ndim.distance_matrix_fast (e.g. window=10 for
                Sakoe-Chiba band constraint).

        Returns:
            NxN distance matrix (squareform), cached under self.distance_matrices
            for compatibility with parent class methods (_calculate_silhouette, etc.).
        """
        if self.windows is None:
            raise RuntimeError("Call create_rolling_windows() first")

        cache_key = f"{metric}_{hash(frozenset(dtw_args.items()))}"
        if cache_key in self.distance_matrices:
            return self.distance_matrices[cache_key]

        n_windows = self.windows.shape[0]
        series = [self.windows[i] for i in range(n_windows)]

        logger.info(
            "Computing %dx%d multivariate DTW distance matrix (%d pairs) ...",
            n_windows,
            n_windows,
            n_windows * (n_windows - 1) // 2,
        )

        dm_compact = dtw_ndim.distance_matrix_fast(
            series,
            ndim=self.n_features,
            compact=True,
            parallel=True,
            **dtw_args,
        )
        self.condensed_dist = np.array(dm_compact)

        # Cache the full NxN matrix for parent class compatibility
        full_matrix = squareform(self.condensed_dist)
        self.distance_matrices[cache_key] = full_matrix

        logger.info("Distance matrix computed, condensed length=%d", len(self.condensed_dist))
        return full_matrix

    def hierarchical_clustering(
        self,
        n_clusters: int,
        metric: str = "dtw",
        linkage_method: str = "complete",
        **dtw_args,
    ) -> dict:
        """Apply hierarchical clustering on the DTW distance matrix.

        Overrides the parent method to fix bug #3:
          The parent passes the NxN squareform matrix to linkage(), which interprets
          2D input as observation vectors and computes ||d_i - d_j|| instead of using
          d_ij directly. The fix passes the condensed distance vector instead.

        Args:
            n_clusters: Number of clusters to cut the dendrogram into (2-4).
            metric: Distance metric (only 'dtw' supported).
            linkage_method: Linkage criterion ('complete', 'average', 'single').
                Ward is not supported with DTW distances.
            **dtw_args: Passed to _compute_distance_matrix.

        Returns:
            Dict with keys: linkage_matrix, labels, silhouette_score, n_clusters.
        """
        if linkage_method == "ward":
            raise ValueError("Ward linkage requires Euclidean distances; use 'complete' or 'average' with DTW")

        # Compute distance matrix (populates self.condensed_dist)
        distance_matrix = self._compute_distance_matrix(metric, **dtw_args)

        # Bug fix #3: pass condensed vector to linkage(), NOT the NxN squareform
        Z = linkage(self.condensed_dist, method=linkage_method)
        labels = fcluster(Z, n_clusters, criterion="maxclust")

        # Silhouette score using precomputed full distance matrix
        sil = self._calculate_silhouette(labels, distance_matrix, "precomputed")

        logger.info("Clustering k=%d: silhouette=%.4f", n_clusters, sil)

        return {
            "linkage_matrix": Z,
            "clusters": labels,
            "silhouette_score": sil,
            "n_clusters": len(np.unique(labels)),
        }

    def _calculate_silhouette(
        self,
        clusters: np.ndarray,
        distance_matrix: np.ndarray,
        metric: str,
    ) -> float:
        """Silhouette score from precomputed distance matrix.

        Overrides parent to always use precomputed metric with the full DTW
        distance matrix.
        """
        if len(np.unique(clusters)) < 2:
            return np.nan

        mask = clusters != -1 if -1 in clusters else slice(None)
        return silhouette_score(
            distance_matrix[mask][:, mask] if isinstance(mask, np.ndarray) else distance_matrix,
            clusters[mask] if isinstance(mask, np.ndarray) else clusters,
            metric="precomputed",
        )

    def map_labels_to_timeline(self, labels: np.ndarray) -> pd.DataFrame:
        """Map cluster labels back to the original date timeline.

        Each label is assigned to the last date of its rolling window.

        Args:
            labels: Cluster labels array of length n_windows.

        Returns:
            DataFrame with columns [date, regime_cluster].
        """
        if self.window_dates is None:
            raise RuntimeError("Call create_rolling_windows() first")

        return pd.DataFrame(
            {
                "date": self.window_dates,
                "regime_cluster": labels,
            }
        )

    def analyze_clusters(self, clusters: np.ndarray) -> pd.DataFrame:
        """Compute per-cluster statistics over the windowed features.

        Overrides parent because the parent indexes self.raw_data (T rows) with
        window labels (n_windows items). This version operates on self.windows.

        Args:
            clusters: Cluster labels array of length n_windows.

        Returns:
            DataFrame with per-cluster mean, std, and count.
        """
        if self.windows is None:
            raise RuntimeError("Call create_rolling_windows() first")

        rows = []
        for cid in sorted(np.unique(clusters)):
            mask = clusters == cid
            cluster_windows = self.windows[mask]  # (n_in_cluster, window_size, n_features)
            flat = cluster_windows.reshape(-1, self.n_features)
            row = {"cluster": int(cid), "n_windows": int(mask.sum())}
            for i, name in enumerate(self.feature_names):
                row[f"{name}_mean"] = float(flat[:, i].mean())
                row[f"{name}_std"] = float(flat[:, i].std())
            rows.append(row)

        return pd.DataFrame(rows)

    def plot_dendrogram_to_file(
        self,
        Z: np.ndarray,
        n_clusters: int,
        output_path: Path,
    ) -> None:
        """Save dendrogram with a color threshold matching n_clusters.

        Uses a separate name from the parent's plot_dendrogram to avoid
        signature conflict (parent calls plt.show(), this saves to file).

        Args:
            Z: Linkage matrix from hierarchical_clustering().
            n_clusters: Number of clusters (used to set color threshold).
            output_path: Path to save the PNG.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Determine color threshold: the merge distance just below cutting into n_clusters
        if n_clusters < Z.shape[0] + 1:
            color_threshold = Z[-(n_clusters - 1), 2]
        else:
            color_threshold = 0

        fig, ax = plt.subplots(figsize=(16, 7))
        dendrogram(
            Z,
            ax=ax,
            orientation="top",
            color_threshold=color_threshold,
            above_threshold_color="grey",
            no_labels=True,
        )
        ax.set_title(f"Hierarchical DTW Clustering Dendrogram (k={n_clusters})", fontsize=14, fontweight="bold")
        ax.set_xlabel("Window Index")
        ax.set_ylabel("DTW Distance")
        ax.axhline(y=color_threshold, color="red", linestyle="--", alpha=0.7, label=f"Cut at k={n_clusters}")
        ax.legend(loc="best")
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        plt.close()
        logger.info("Dendrogram saved to %s", output_path)

    def plot_regime_timeline(
        self,
        labels_df: pd.DataFrame,
        output_path: Path,
    ) -> None:
        """Overlay cluster labels on SPX price chart.

        Args:
            labels_df: DataFrame with [date, regime_cluster] from map_labels_to_timeline.
            output_path: Path to save the PNG.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Load SPX prices
        spx = pd.read_parquet(SPX_PATH)
        if "index" in spx.columns:
            spx = spx.rename(columns={"index": "date"})
        spx["date"] = pd.to_datetime(spx["date"])

        # Merge with labels
        merged = spx.merge(labels_df, on="date", how="inner")
        if merged.empty:
            logger.warning("No overlapping dates between SPX and labels; skipping timeline plot")
            return

        merged = merged.sort_values("date").reset_index(drop=True)
        merged = merged.set_index("date")

        fig, ax = plt.subplots(figsize=(14, 6))

        # Background shading by regime
        current_regime = None
        regime_start = None
        for idx, row in merged.iterrows():
            regime = int(row["regime_cluster"])
            if regime != current_regime:
                if current_regime is not None and regime_start is not None:
                    color = REGIME_COLORS.get(current_regime, "#95a5a6")
                    ax.axvspan(regime_start, idx, alpha=0.25, color=color, zorder=1)
                current_regime = regime
                regime_start = idx
        # Final block
        if current_regime is not None and regime_start is not None:
            color = REGIME_COLORS.get(current_regime, "#95a5a6")
            ax.axvspan(regime_start, merged.index[-1], alpha=0.25, color=color, zorder=1)

        # SPX price line
        ax.plot(merged.index, merged["close_SPY"], color="black", linewidth=1.5, label="SPY Close", zorder=10)

        # Legend for regimes
        for cid in sorted(merged["regime_cluster"].unique()):
            color = REGIME_COLORS.get(int(cid), "#95a5a6")
            ax.axvspan(merged.index[0], merged.index[0], alpha=0.25, color=color, label=f"Regime {int(cid)}")

        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("SPY Close Price", fontsize=12)
        ax.set_title("SPX Price with DTW Regime Clusters", fontsize=14, fontweight="bold")
        ax.legend(loc="best", fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        svg_path = output_path.with_suffix(".svg")
        plt.savefig(svg_path, bbox_inches="tight")
        plt.close()
        logger.info("Regime timeline saved to %s", output_path)


def main() -> None:
    """Run the full DTW clustering pipeline."""
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Load PCA features
    logger.info("Loading HMM input from %s", HMM_INPUT_PATH)
    df = pd.read_parquet(HMM_INPUT_PATH)

    # 2. Initialize and create windows
    rc = RegimeClustering(df, window_size=21)
    rc.create_rolling_windows()

    # 3. Compute DTW distance matrix
    rc._compute_distance_matrix()

    # 4. Cluster for k=2,3,4 and find best silhouette
    results = {}
    for k in (2, 3, 4):
        res = rc.hierarchical_clustering(n_clusters=k, linkage_method="complete")
        results[k] = res

        # Plot dendrogram
        rc.plot_dendrogram_to_file(
            res["linkage_matrix"],
            n_clusters=k,
            output_path=FIGURES_DIR / f"dtw_dendrogram_k{k}.png",
        )

<<<<<<< HEAD
    # 5. Select best k by silhouette score, applying penalties to favor k=3
    k_penalties = {2: 0.05, 3: 0.0, 4: 0.05}
    best_k = max(results, key=lambda k: results[k]["silhouette_score"] - k_penalties.get(k, 0.0))
=======
    # 5. Select best k by silhouette score
    best_k = max(results, key=lambda k: results[k]["silhouette_score"])
>>>>>>> d088731 (feat: regime detection via dtw clustering)
    best_res = results[best_k]
    logger.info(
        "Best k=%d (silhouette=%.4f). All scores: %s",
        best_k,
        best_res["silhouette_score"],
        {k: f"{r['silhouette_score']:.4f}" for k, r in results.items()},
    )

    # 6. Map labels to timeline
    labels_df = rc.map_labels_to_timeline(best_res["clusters"])

    # 7. Plot SPX overlay
    rc.plot_regime_timeline(labels_df, output_path=FIGURES_DIR / "dtw_regime_timeline.png")

    # 8. Analyze clusters (uses parent's analyze_clusters)
    stats_df = rc.analyze_clusters(best_res["clusters"])
    stats_path = REPORTS_DIR / "dtw_cluster_stats.csv"
    stats_df.to_csv(stats_path, index=False)
    logger.info("Cluster statistics saved to %s", stats_path)
    logger.info("\n%s", stats_df.to_string())

    # 9. Save labels
    labels_path = PROCESSED_DIR / "dtw_regime_labels.csv"
    labels_df.to_csv(labels_path, index=False)
    logger.info("Regime labels saved to %s", labels_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
