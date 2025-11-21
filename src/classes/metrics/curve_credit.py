import logging
from pathlib import Path

import pandas as pd


def build_paths() -> tuple[Path, Path, Path, Path]:
    """Build paths to cleaned input CSVs and processed parquet output.

    Assumes the directory structure:

        project_root/
            data/
                cleaned/
                    dgs10.csv
                    dgs2.csv
                    credit_spread.csv
                processed/
                    curve_credit_features.parquet
            src/
                classes/
                    metrics/
                        curve_credit.py

    Returns:
        tuple[Path, Path, Path, Path]: Paths to dgs10.csv, dgs2.csv,
        credit_spread.csv, and curve_credit_features.parquet.
    """
    root_dir = Path(__file__).resolve().parents[3]
    cleaned_dir = root_dir / "data" / "cleaned"
    processed_dir = root_dir / "data" / "processed"

    dgs10_path = cleaned_dir / "dgs10.csv"
    dgs2_path = cleaned_dir / "dgs2.csv"
    credit_spread_path = cleaned_dir / "credit_spread.csv"
    output_path = processed_dir / "curve_credit_features.parquet"

    return dgs10_path, dgs2_path, credit_spread_path, output_path


def load_series(
    path: Path, date_col: str = "date", value_col: str = "value"
) -> pd.DataFrame:
    """Load a time series CSV with a date and value column.

    Args:
        path (Path): Path to the CSV file.
        date_col (str): Name of the date column. Defaults to "date".
        value_col (str): Name of the value column. Defaults to "value".

    Returns:
        pd.DataFrame: DataFrame with parsed dates, sorted by date.
    """
    df = pd.read_csv(path, parse_dates=[date_col])
    df = df[[date_col, value_col]].sort_values(date_col).reset_index(drop=True)
    return df


def compute_curve_slope(
    dgs10: pd.DataFrame, dgs2: pd.DataFrame
) -> pd.DataFrame:
    """Compute the 10-year minus 2-year Treasury yield slope.

    Args:
        dgs10 (pd.DataFrame): DataFrame containing 10-year yields with
            columns ['date', 'value'].
        dgs2 (pd.DataFrame): DataFrame containing 2-year yields with
            columns ['date', 'value'].

    Returns:
        pd.DataFrame: DataFrame containing:
            - date
            - curve_10y_2y (difference DGS10 - DGS2)
    """
    merged = dgs10.merge(
        dgs2,
        on="date",
        how="inner",
        suffixes=("_10y", "_2y"),
    )

    features = merged[["date"]].copy()
    features["curve_10y_2y"] = merged["value_10y"] - merged["value_2y"]
    return features


def prepare_credit_spread(credit_spread: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill the credit spread series on its native dates.

    Args:
        credit_spread (pd.DataFrame): DataFrame with columns ['date', 'value'].

    Returns:
        pd.DataFrame: Forward-filled credit spread, indexed by date, with
        column 'credit_spread_baa_aaa'.
    """
    cs = credit_spread.copy().sort_values("date").set_index("date")
    cs["credit_spread_baa_aaa"] = cs["value"]
    cs = cs[["credit_spread_baa_aaa"]].ffill()
    return cs


def build_curve_credit_features() -> pd.DataFrame:
    """Build merged features: 10y-2y curve slope + forward-filled credit spread.

    Uses merge_asof to align monthly credit spreads with daily curve dates
    by carrying the last observed credit spread value forward.

    Returns:
        pd.DataFrame: Features with columns:
            - date
            - curve_10y_2y
            - credit_spread_baa_aaa

    Raises:
        ValueError: If missing credit spread values remain after merge.
    """
    dgs10_path, dgs2_path, credit_spread_path, _ = build_paths()

    logging.info("Loading DGS10, DGS2 and credit spread series.")
    dgs10 = load_series(dgs10_path)
    dgs2 = load_series(dgs2_path)
    credit_spread = load_series(credit_spread_path)

    logging.info("Computing 10y-2y curve slope.")
    curve = compute_curve_slope(dgs10, dgs2).sort_values("date")

    logging.info("Preparing credit spread series.")
    credit = (
        credit_spread.sort_values("date")[["date", "value"]]
        .rename(columns={"value": "credit_spread_baa_aaa"})
    )

    logging.info("Merging curve slope with credit spreads using merge_asof.")
    features = pd.merge_asof(
        curve,
        credit,
        on="date",
        direction="backward",
    )

    if features["credit_spread_baa_aaa"].isna().any():
        raise ValueError("Missing credit_spread_baa_aaa values after merge_asof")

    return features


def save_curve_credit_features(df: pd.DataFrame) -> None:
    """Save curve + credit features to parquet.

    Args:
        df (pd.DataFrame): Features DataFrame to save.
    """
    _, _, _, output_path = build_paths()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("Saving curve_credit_features.parquet to %s", output_path)
    df.to_parquet(output_path, index=False)


def main() -> None:
    """Main entry point for generating and saving curve+credit features."""
    logging.info("Building curve_credit_features dataframe.")
    features = build_curve_credit_features()
    save_curve_credit_features(features)
    logging.info("Completed curve_credit feature generation.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
