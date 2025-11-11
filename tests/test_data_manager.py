"""
Unified test suite for the DataManager class.
Includes unit tests, integration tests, and comprehensive cleaning/validation tests.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "classes" / "data"))
from data_manager import DataManager


# ==================== UNIT TESTS ====================

def test_forward_fill_missing_data():
    df = pd.DataFrame({"a": [1, None, 3]})
    manager = DataManager()
    filled = manager.forward_fill_missing_data(df)
    assert filled["a"].isnull().sum() == 0


def test_percent_to_decimal():
    df = pd.DataFrame({"value": [100, 50, 25]})
    manager = DataManager()
    out = manager.percent_to_decimal(df, ["value"])
    assert all(out["value"] < 2)


def test_check_anomalies_macroeconomic():
    df = pd.DataFrame({"date": ["2020-01-01", "2020-01-02"], "value": [1, 2]})
    manager = DataManager()
    # Should not raise
    manager.check_anomalies_macroeconomic(df)
    df2 = df.copy()
    df2.loc[0, "value"] = -1
    with pytest.raises(ValueError):
        manager.check_anomalies_macroeconomic(df2)
    # Duplicate date should raise
    df3 = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError):
        manager.check_anomalies_macroeconomic(df3)
    # Null value should raise
    df4 = df.copy()
    df4.loc[0, "value"] = None
    with pytest.raises(ValueError):
        manager.check_anomalies_macroeconomic(df4)


def test_check_time_gaps():
    manager = DataManager()
    # No gap
    df = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=3, freq="B"), "value": [1, 2, 3]})
    manager.check_time_gaps(df)
    # Gap
    df2 = df.drop(1)
    with pytest.raises(ValueError):
        manager.check_time_gaps(df2)


def test_create_rows_for_missing_dates():
    manager = DataManager()
    df = pd.DataFrame({"date": ["2020-01-01", "2020-01-03"], "value": [1, 3]})
    out = manager.create_rows_for_missing_dates(df)
    assert pd.Timestamp("2020-01-02") in out.index


def test_group_by_date():
    manager = DataManager()
    df = pd.DataFrame(
        {
            "date": ["2020-01-01 09:30", "2020-01-01 16:00"],
            "open": [1, 2],
            "high": [2, 3],
            "low": [0, 1],
            "close": [2, 3],
            "volume": [100, 200],
        }
    )
    out = manager.group_by_date(df)
    assert out.shape[0] == 1
    assert out["open"].iloc[0] == 1
    assert out["close"].iloc[0] == 3
    assert out["volume"].iloc[0] == 300


def test_missing_dates():
    manager = DataManager()
    df = pd.DataFrame({"date": ["2020-01-01", "2020-01-03"], "value": [1, 3]})
    missing = manager.missing_dates(df)
    assert pd.Timestamp("2020-01-02") in missing


# ==================== INTEGRATION TESTS ====================

def test_data_manager_initialization():
    """Test that DataManager initializes correctly."""
    manager = DataManager()
    assert manager.data_dir is not None
    assert manager.raw_dir is not None
    assert manager.cleaned_dir is not None
    assert manager.data == {}
    assert manager.merged_data is None


def test_data_manager_dir_creation():
    """Test that DataManager creates required directories."""
    manager = DataManager()
    assert manager.raw_dir.exists()
    assert manager.cleaned_dir.exists()


def test_cleaning_pipeline():
    """Test a complete cleaning pipeline for macroeconomic data."""
    manager = DataManager()
    
    # Create sample data
    df = pd.DataFrame({
        "date": pd.date_range("2020-01-01", periods=10),
        "value": [1.0, 2.0, None, 3.0, 4.0, 5.0, None, 6.0, 7.0, 8.0]
    })
    
    # Apply cleaning steps
    df_cleaned = manager.forward_fill_missing_data(df)
    assert df_cleaned.isnull().sum().sum() == 0
    
    # Ensure data is in proper format
    assert "date" in df_cleaned.columns
    assert "value" in df_cleaned.columns


def test_validation_pipeline():
    """Test data validation pipeline."""
    manager = DataManager()
    
    # Create valid data
    valid_df = pd.DataFrame({
        "date": pd.date_range("2020-01-01", periods=5),
        "value": [1.0, 2.0, 3.0, 4.0, 5.0]
    })
    
    # Should not raise
    manager.check_anomalies_macroeconomic(valid_df)
    
    # Create invalid data (negative values)
    invalid_df = valid_df.copy()
    invalid_df.loc[0, "value"] = -1.0
    
    with pytest.raises(ValueError):
        manager.check_anomalies_macroeconomic(invalid_df)


def test_fred_api_initialization():
    """Test that FRED API methods are available."""
    manager = DataManager()
    assert hasattr(manager, 'fetch_fred_series')
    assert hasattr(manager, 'fetch_and_save_fred_data')
    assert callable(manager.fetch_fred_series)
    assert callable(manager.fetch_and_save_fred_data)


def test_csv_save_and_load():
    """Test CSV save and load functionality."""
    manager = DataManager()
    
    # Create sample data
    df = pd.DataFrame({"date": ["2020-01-01", "2020-01-02"], "value": [1.0, 2.0]})
    
    # Save it
    manager._save_csv(df, "test_data")
    
    # Load it
    loaded_df = manager.load_csv("test_data.csv", name="test_loaded")
    
    # Verify
    assert loaded_df.equals(df) or loaded_df.reset_index(drop=True).equals(df.reset_index(drop=True))
    assert "test_loaded" in manager.data
