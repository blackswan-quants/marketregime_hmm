"""
Unit Tests for Task P6: Labels & Sanity Plots

Tests cover:
  1. Volatility bucket logic
  2. Risk regime heuristic rules
  3. Output file generation and format
"""

import unittest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import sys

# Add src to path
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from src.labels.make_labels import (
    compute_volatility_buckets,
    compute_risk_regimes,
    make_labels,
)


class TestVolatilityBuckets(unittest.TestCase):
    """Test volatility bucket generation."""
    
    def setUp(self):
        """Create sample volatility data."""
        np.random.seed(42)
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        self.df = pd.DataFrame({
            "vol": np.random.gamma(shape=2, scale=2, size=100),
        }, index=dates)
    
    def test_bucket_counts(self):
        """Verify tertile split creates roughly equal counts."""
        buckets = compute_volatility_buckets(self.df, "vol")
        
        # Check all three buckets are present
        self.assertEqual(len(buckets.value_counts()), 3)
        
        # Check no NaNs
        self.assertEqual(buckets.isna().sum(), 0)
        
        # Check approximately equal distribution (±20%)
        counts = buckets.value_counts()
        expected_count = len(self.df) // 3
        for count in counts.values:
            self.assertAlmostEqual(count, expected_count, delta=expected_count * 0.2)
    
    def test_bucket_labels(self):
        """Verify bucket labels are correct."""
        buckets = compute_volatility_buckets(self.df, "vol")
        expected_labels = {"Low_Vol", "Mid_Vol", "High_Vol"}
        self.assertEqual(set(buckets.unique()), expected_labels)
    
    def test_ordering(self):
        """Verify Low < Mid < High in terms of volatility values."""
        buckets = compute_volatility_buckets(self.df, "vol")
        
        low_vol = self.df.loc[buckets == "Low_Vol", "vol"].mean()
        mid_vol = self.df.loc[buckets == "Mid_Vol", "vol"].mean()
        high_vol = self.df.loc[buckets == "High_Vol", "vol"].mean()
        
        self.assertLess(low_vol, mid_vol)
        self.assertLess(mid_vol, high_vol)


class TestRiskRegimes(unittest.TestCase):
    """Test risk regime heuristic rules."""
    
    def setUp(self):
        """Create sample feature data."""
        np.random.seed(42)
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        
        self.df = pd.DataFrame({
            "mom_10": np.random.uniform(-0.02, 0.02, 100),
            "mom_42": np.random.uniform(-0.05, 0.05, 100),
            "vix_ratio": np.random.uniform(0.5, 1.5, 100),
            "yc_slope": np.random.uniform(-0.02, 0.02, 100),
            "credit_change": np.random.uniform(-0.01, 0.01, 100),
        }, index=dates)
        
        self.config = {
            "momentum_short": "mom_10",
            "momentum_long": "mom_42",
            "vix_ewma_ratio": "vix_ratio",
            "yc_slope": "yc_slope",
            "credit_spread_change": "credit_change",
        }
    
    def test_regime_labels(self):
        """Verify regime labels are valid."""
        regimes = compute_risk_regimes(self.df, self.config)
        expected_labels = {"Risk-On", "Risk-Off", "Neutral"}
        self.assertLessEqual(set(regimes.unique()), expected_labels)
        self.assertGreater(len(regimes.unique()), 0)
    
    def test_no_nans(self):
        """Verify no NaN values in output."""
        regimes = compute_risk_regimes(self.df, self.config)
        self.assertEqual(regimes.isna().sum(), 0)
    
    def test_risk_on_condition(self):
        """Verify Risk-On condition logic."""
        # Create rows that should be Risk-On
        df_test = pd.DataFrame({
            "mom_10": [0.01],  # Positive momentum
            "mom_42": [0.01],  # Doesn't matter for Risk-On, but make it positive
            "vix_ratio": [1.0],  # Less than 1.1
            "yc_slope": [0.01],  # Positive (no stress)
            "credit_change": [-0.01],  # Negative (no stress)
        }, index=pd.date_range("2024-01-01", periods=1))
        
        regimes = compute_risk_regimes(df_test, self.config)
        self.assertEqual(regimes.iloc[0], "Risk-On")
    
    def test_risk_off_condition(self):
        """Verify Risk-Off condition logic (OR - any trigger)."""
        # Test: momentum_42 < 0
        df_test = pd.DataFrame({
            "mom_10": [0.01],
            "mom_42": [-0.01],  # Negative → Risk-Off
            "vix_ratio": [1.0],
            "yc_slope": [0.01],
            "credit_change": [-0.01],
        }, index=pd.date_range("2024-01-01", periods=1))
        
        regimes = compute_risk_regimes(df_test, self.config)
        self.assertEqual(regimes.iloc[0], "Risk-Off")


class TestPipeline(unittest.TestCase):
    """Test end-to-end pipeline."""
    
    def test_make_labels_with_valid_data(self):
        """Test full pipeline with sample data."""
        # Create temporary parquet file
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            
            # Create sample data matching expected columns
            dates = pd.date_range("2024-01-01", periods=100, freq="D")
            df = pd.DataFrame({
                "vol_ewma_20": np.random.gamma(2, 2, 100),
                "spx_mom_10": np.random.uniform(-0.02, 0.02, 100),
                "spx_mom_42": np.random.uniform(-0.05, 0.05, 100),
                "vix_ewma_ratio": np.random.uniform(0.5, 1.5, 100),
                "yc_slope_10_2": np.random.uniform(-0.02, 0.02, 100),
                "credit_spread_diff_21": np.random.uniform(-0.01, 0.01, 100),
                "spx_ret_1d": np.random.normal(0.0005, 0.015, 100),
            }, index=dates)
            df.index.name = "date"
            
            # Save to parquet
            input_path = tmpdir / "features.parquet"
            df.to_parquet(input_path)
            
            output_path = tmpdir / "labels.csv"
            
            # Run pipeline
            labels_df = make_labels(input_path=input_path, output_path=output_path)
            
            # Verify output
            self.assertEqual(labels_df.shape[0], 100)
            self.assertEqual(labels_df.shape[1], 2)
            self.assertIn("vol_bucket", labels_df.columns)
            self.assertIn("risk_regime", labels_df.columns)
            self.assertEqual(labels_df.isna().sum().sum(), 0)
            
            # Verify CSV was written
            self.assertTrue(output_path.exists())


if __name__ == "__main__":
    unittest.main()
