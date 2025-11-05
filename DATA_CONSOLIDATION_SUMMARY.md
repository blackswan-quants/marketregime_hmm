# Data Team Scripts Consolidation Summary

## Overview
All data team scripts have been merged into a single, unified `DataManager` class located in `src/classes/data/manager.py`.

## What Was Merged

### 1. **fetch_data.py** → DataManager
- `fetch_fred_series()` - Fetch individual FRED series with retry logic
- `fetch_and_save_fred_data()` - Main API fetching orchestrator
- `_derive_credit_spread()` - Calculate BAA-AAA credit spread
- `_derive_10y_2y_spread()` - Calculate 10Y-2Y yield curve spread

### 2. **clean_data.py** → DataManager
- `forward_fill_missing_data()` - Forward fill missing values
- `percent_to_decimal()` - Convert percentages to decimals
- `check_anomalies_macroeconomic()` - Validate macro data quality
- `check_time_gaps()` - Verify business day continuity
- `create_rows_for_missing_dates()` - Fill missing business days
- `group_by_date()` - Aggregate intraday data to daily OHLCV
- `missing_dates()` - Identify missing business dates
- `clean_macro_data()` - Pipeline for macro data cleaning
- `clean_price_data()` - Pipeline for price data cleaning

### 3. **loader.py** → DataManager (was empty)
- `load_csv()` - Load CSV files
- `load_all_raw_data()` - Load all raw data files

## File Structure

```
src/classes/data/
├── __init__.py           # Package initialization, exports DataManager
├── manager.py            # Unified DataManager class (PRIMARY FILE)
├── loader.py             # Re-exports DataManager for backward compatibility
└── cleaner.py            # Re-exports DataManager for backward compatibility
```

## Key Features

### Data Fetching
```python
manager = DataManager()
manager.fetch_and_save_fred_data(api_key="your_fred_key", months=12)
```

### Data Cleaning & Processing
```python
# Load and clean all data in one pipeline
merged_df = manager.merge_market_and_macro_data()

# Save results
manager.save_processed_data()
```

### Individual Methods Available
- **Macro data cleaning**: `clean_macro_data(df, name, check_gaps=False)`
- **Price data cleaning**: `clean_price_data(df, name)`
- **Data validation**: `check_anomalies_macroeconomic()`, `check_time_gaps()`
- **Data transformation**: `group_by_date()`, `create_rows_for_missing_dates()`
- **Data loading**: `load_csv()`, `load_all_raw_data()`

## Usage Examples

### Complete Pipeline
```python
from src.classes.data import DataManager

manager = DataManager()
merged_df = manager.merge_market_and_macro_data()
manager.save_processed_data()
info = manager.get_data_info()
```

### Access Individual Methods
```python
# Load raw data
spy = manager.load_csv("SPY_1min_20231027_20251027.csv")

# Clean price data
spy_clean = manager.clean_price_data(spy, "SPY")

# Check for issues
manager.check_time_gaps(spy_clean)
```

## Configuration

### Default Paths
- Raw data: `data/raw/`
- Processed data: `data/processed/`

### Custom Data Directory
```python
manager = DataManager(data_dir="/path/to/data")
```

## Backward Compatibility

The original `loader.py` and `cleaner.py` files now re-export the `DataManager` class, so any existing imports will continue to work:

```python
# These all work and return the same DataManager class
from src.classes.data import DataManager
from src.classes.data.loader import DataManager
from src.classes.data.cleaner import DataManager
```

## Advantages of Consolidation

1. **Single Source of Truth** - All data operations in one place
2. **Improved State Management** - DataManager maintains data state internally
3. **Better Error Handling** - Centralized validation and error checking
4. **Easier Maintenance** - No code duplication across modules
5. **Streamlined API** - Clean interface for all data operations
6. **Type Hints** - Full type annotations for better IDE support
7. **Documentation** - Comprehensive docstrings for all methods
