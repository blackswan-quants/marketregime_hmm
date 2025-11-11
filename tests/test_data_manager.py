"""
Test suite for the unified DataManager class.
"""
import sys
from pathlib import Path
import pytest
import pandas as pd
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "classes" / "data"))
from data_manager import DataManager

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
    df = pd.DataFrame({
        "date": ["2020-01-01 09:30", "2020-01-01 16:00"],
        "open": [1, 2],
        "high": [2, 3],
        "low": [0, 1],
        "close": [2, 3],
        "volume": [100, 200],
    })
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
