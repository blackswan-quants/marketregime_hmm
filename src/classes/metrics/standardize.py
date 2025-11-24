from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd


class Standardizer:
    """Alignment + standardization utilities for feature engineering."""

    @staticmethod
    def _prepare_frame(df: pd.DataFrame) -> pd.DataFrame:
        """Ensure every frame exposes a `date` column sorted ascending.

        Args:
            df (pd.DataFrame): Raw feature frame with either a ``date`` column or
                a datetime index.

        Returns:
            pd.DataFrame: Copy of ``df`` with an explicit ``date`` column sorted
            chronologically.
        """
        if df.index.name == "date":
            df = df.reset_index()
        if "date" not in df.columns:
            raise ValueError("Input frame needs a 'date' column")
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date").reset_index(drop=True)

    @staticmethod
    def load_and_align(
        filepaths: List[str],
        calendar: str = "intersection",
        return_report: bool = False,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """Load feature files, build a calendar, and merge on date.

        Args:
            filepaths (List[str]): Ordered list of parquet file paths to align.
            calendar (str): ``'intersection'`` or ``'union'`` calendar policy.
            return_report (bool): Whether to also return alignment metadata.

        Returns:
            pd.DataFrame | tuple: Feature table keyed by ``date``. If
            ``return_report`` is ``True`` a tuple ``(frame, report)`` is
            returned where ``report`` captures per-source row counts.
        """
        frames = [Standardizer._prepare_frame(pd.read_parquet(path)) for path in filepaths]
        date_sets = [set(frame["date"]) for frame in frames]
        source_rows: Dict[str, int] = {}
        source_ranges: Dict[str, Dict[str, str]] = {}

        for path, frame in zip(filepaths, frames):
            key = str(path)
            source_rows[key] = len(frame)
            source_ranges[key] = {
                "start": frame["date"].min().strftime("%Y-%m-%d"),
                "end": frame["date"].max().strftime("%Y-%m-%d"),
            }

        if calendar == "intersection":
            common = set.intersection(*date_sets)
        elif calendar == "union":
            common = set.union(*date_sets)
        else:
            raise ValueError("calendar must be 'intersection' or 'union'")

        idx = pd.DatetimeIndex(sorted(common))
        aligned = [frame[frame["date"].isin(idx)] for frame in frames]

        merged = aligned[0]
        for frame in aligned[1:]:
            merged = merged.merge(frame, on="date", how="inner")

        merged = merged.sort_values("date").reset_index(drop=True)

        if not return_report:
            return merged

        aligned_rows = len(idx)
        aligned_range = {
            "start": idx[0].strftime("%Y-%m-%d") if len(idx) else None,
            "end": idx[-1].strftime("%Y-%m-%d") if len(idx) else None,
        }
        rows_dropped = {source: max(count - aligned_rows, 0) for source, count in source_rows.items()}
        report = {
            "alignment_method": calendar,
            "source_rows": source_rows,
            "source_date_ranges": source_ranges,
            "aligned_rows": aligned_rows,
            "aligned_date_range": aligned_range,
            "rows_dropped_by_source": rows_dropped,
        }
        return merged, report

    @staticmethod
    def handle_na(df: pd.DataFrame) -> pd.DataFrame:
        """Drop warm-up rows and enforce NA-free data.

        Args:
            df (pd.DataFrame): Frame returned by :meth:`load_and_align`.

        Returns:
            pd.DataFrame: NA-free subset with the initial warm-up rows removed.
        """
        feature_cols = [col for col in df.columns if col != "date"]
        full_rows = df[feature_cols].notna().all(axis=1)
        if not full_rows.any():
            raise ValueError("No fully populated rows found")
        first_full = full_rows.idxmax()
        return df.loc[first_full:].dropna().reset_index(drop=True)

    def __init__(self):
        """Initialize the helper.

        Standardizer no longer holds state, but the constructor is kept for API
        compatibility with previous iterations.
        """
        pass

    def fit_transform(
        self,
        df: pd.DataFrame,
        exclude: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        """Add `_z` columns computed with in-sample mean and std.

        Args:
            df (pd.DataFrame): Feature frame containing numeric columns to
                standardize.
            exclude (Iterable[str]): Column names to skip (e.g., identifiers).

        Returns:
            pd.DataFrame: Copy of ``df`` containing both the raw columns and the
            standardized ``_z`` counterparts.
        """
        skip = set(exclude or ("date",))
        result = df.copy()
        numeric = result.select_dtypes(include="number").columns
        targets = [col for col in numeric if col not in skip and not col.endswith("_z")]

        for col in targets:
            mu = result[col].mean()
            sigma = result[col].std(ddof=0)
            if sigma == 0:
                result[f"{col}_z"] = 0.0
            else:
                result[f"{col}_z"] = (result[col] - mu) / sigma

        return result
