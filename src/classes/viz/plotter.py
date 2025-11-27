import logging
from pathlib import Path
from typing import Iterable, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


class Plotter:
    """
    Generate visualizations for features and analysis results.
    Usage:
        plotter = Plotter()
        plotter.correlation_heatmap(
            features_df,
            output_path='data/processed/corr_heatmap.png'
        )
    """

    def __init__(self, style: str = "darkgrid", context: str = "notebook"):
        """
        Initialize Plotter with seaborn style settings.
        Args:
            style: Seaborn style ('darkgrid', 'whitegrid', 'dark', 'white', 'ticks')
            context: Seaborn context ('paper', 'notebook', 'talk', 'poster')
        """
        sns.set_style(style)
        sns.set_context(context)

    def correlation_heatmap(
        self,
        df: pd.DataFrame,
        output_path: str,
        exclude_cols: Iterable[str] = ("date",),
        figsize: tuple = (16, 14),
        annot: bool = True,
        mask_upper: bool = False,
        cmap: str = "coolwarm",
        vmin: float = -1.0,
        vmax: float = 1.0,
        title: str = "Feature Correlation Heatmap",
    ) -> None:
        """
        Generate and save correlation heatmap.
        Args:
            df: DataFrame with features
            output_path: Path to save PNG file
            exclude_cols: Columns to exclude
            figsize: Figure size (width, height)
            annot: Whether to annotate cells with correlation values
            mask_upper: Whether to mask upper triangle
            cmap: Colormap name
            vmin: Minimum value for colormap
            vmax: Maximum value for colormap
            title: Plot title
        """
        exclude_set = set(exclude_cols)

        # Select numeric columns, excluding specified ones
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        feature_cols = [col for col in numeric_cols if col not in exclude_set]

        # Compute correlation matrix and reorder columns by average abs corr
        corr_matrix = df[feature_cols].corr()
        ordered_cols = corr_matrix.abs().mean().sort_values(ascending=False).index.tolist()
        corr_matrix = corr_matrix.loc[ordered_cols, ordered_cols]

        # Create mask for upper triangle if requested
        mask = None
        if mask_upper:
            mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)

        # Create figure
        fig, ax = plt.subplots(figsize=figsize)

        # Generate heatmap
        sns.heatmap(
            corr_matrix,
            mask=mask,
            annot=annot,
            fmt=".2f",
            annot_kws={"size": 7},
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            center=0,
            square=True,
            linewidths=0.5,
            cbar_kws={"shrink": 0.8, "label": "Correlation"},
            ax=ax,
        )

        # Set title and labels
        ax.set_title(title, fontsize=16, pad=20)
        ax.set_xlabel("")
        ax.set_ylabel("")

        # Rotate labels
        plt.xticks(rotation=45, ha="right")
        plt.yticks(rotation=0)

        # Adjust layout
        plt.tight_layout()

        # Save figure
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        plt.close()

        logging.getLogger(__name__).info(f"Correlation heatmap saved to {output_path}")

    def feature_distributions(
        self,
        df: pd.DataFrame,
        output_path: str,
        exclude_cols: Iterable[str] = ("date",),
        n_cols: int = 4,
        figsize_per_subplot: tuple = (4, 3),
        bins: Union[int, str] = "auto",
    ) -> None:
        """
        Generate distribution plots for all features.
        Args:
            df: DataFrame with features
            output_path: Path to save PNG file
            exclude_cols: Columns to exclude
            n_cols: Number of columns in subplot grid
            figsize_per_subplot: Size of each subplot
            bins: Histogram bin specification passed to ``matplotlib``
        """
        exclude_set = set(exclude_cols)

        # Select numeric columns
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        feature_cols = [col for col in numeric_cols if col not in exclude_set]

        # Calculate grid dimensions
        n_features = len(feature_cols)
        n_rows = (n_features + n_cols - 1) // n_cols

        # Create figure
        figsize = (figsize_per_subplot[0] * n_cols, figsize_per_subplot[1] * n_rows)
        fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
        axes = axes.flatten() if n_features > 1 else [axes]

        # Plot each feature
        for idx, col in enumerate(feature_cols):
            ax = axes[idx]
            df[col].hist(bins=bins, ax=ax, edgecolor="black", alpha=0.7)
            ax.set_title(col, fontsize=10)
            ax.set_xlabel("")
            ax.set_ylabel("Frequency")
            ax.grid(alpha=0.3)

        # Hide unused subplots
        for idx in range(n_features, len(axes)):
            axes[idx].axis("off")

        # Adjust layout
        plt.tight_layout()

        # Save figure
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        plt.close()

        logging.getLogger(__name__).info(f"Feature distributions saved to {output_path}")
