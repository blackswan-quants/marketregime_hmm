"""
Main entry point - demonstrates usage of the unified DataManager.

This script shows how to use the DataManager class for loading, cleaning,
and processing market and macroeconomic data.
"""

import os
from src.classes.data import DataManager


def main():
    """Main execution."""
    # Initialize the DataManager
    manager = DataManager()
    
    print("=" * 70)
    print("Data Processing Pipeline - Market & Macro Data")
    print("=" * 70)
    
    # Option 1: Fetch fresh data from FRED API
    # Uncomment to fetch new data (requires FRED_API_KEY environment variable)
    api_key = os.getenv("FRED_API_KEY")
    if api_key:
        print("\n[STEP 1] Fetching fresh data from FRED API...")
        manager.fetch_and_save_fred_data(api_key, months=12)
    else:
        print("\n[STEP 1] Skipping FRED fetch (FRED_API_KEY not set)")
        print("         Using existing data from data/raw/")
    
    # Option 2: Load existing data and run full processing pipeline
    print("\n[STEP 2] Loading and cleaning all market & macro data...")
    merged_df = manager.merge_market_and_macro_data()
    
    # Display data information
    print("\n[STEP 3] Data Summary:")
    info = manager.get_data_info()
    print(f"  Shape: {info['shape']}")
    print(f"  Columns ({len(info['columns'])}): {', '.join(info['columns'])}")
    print(f"  Memory Usage: {info['memory_usage_mb']:.2f} MB")
    if info['date_range'] != "N/A":
        print(f"  Date Range: {info['date_range'][0]} to {info['date_range'][1]}")
    
    # Save processed data
    print("\n[STEP 4] Saving processed data...")
    manager.save_processed_data()
    
    print("\n" + "=" * 70)
    print("Pipeline Complete!")
    print("=" * 70)
    
    return merged_df


if __name__ == "__main__":
    df = main()
