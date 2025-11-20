# Task P6: Labels & Sanity Plots

## Overview

Task P6 is the final step in the Phase F2 feature engineering pipeline. It:

1. **Generates regime labels** from the feature engineering output (P2)
2. **Creates diagnostic plots** to validate the regimes
3. **Produces an acceptance report** with sanity checks

## Prerequisites

- ✅ Task P2 completed (features_dataset.parquet or equivalent)
- ✅ All dependencies installed (`uv sync`)

## Project Structure

```
src/
  ├── labels/
  │   └── make_labels.py          # Label generation logic
  ├── reports/
  │   └── sanity_plots.py         # Plotting and reporting
  └── p6_pipeline.py              # Orchestration script

tests/
  └── test_p6_labels.py           # Unit tests

data/processed/
  ├── features_dataset.parquet    # Input from P2 [NOT YET]
  ├── labels_prelim.csv           # Output: generated labels
  └── f2_acceptance.md            # Output: acceptance report

reports/figures/
  ├── regime_timeline.png         # Output: timeline plot
  └── regime_stats.png            # Output: statistics plot
```

## Scripts

### 1. `src/labels/make_labels.py`

**Purpose:** Generate regime labels

**Configuration:**
- Update the `FEATURE_COLUMNS` dict once P2 completes
- Column names will be provided in the features_dataset.parquet

**Usage:**
```python
from src.labels.make_labels import make_labels

labels_df = make_labels()
```

**Output:** `data/processed/labels_prelim.csv`
- Index: Date (DatetimeIndex)
- Columns: `vol_bucket`, `risk_regime`

**Label Definitions:**

#### Volatility Buckets (3 States)
- **Low_Vol:** Below 33rd percentile of EWMA volatility
- **Mid_Vol:** Between 33rd and 66th percentile
- **High_Vol:** Above 66th percentile

#### Risk Regimes (3 States)
- **Risk-On:** `(momentum_short > 0) AND (vix_ewma_ratio < 1.1)`
- **Risk-Off:** `(momentum_42 < 0) OR (yc_slope < 0) OR (credit_change > 0)` [Priority: Safety first]
- **Neutral:** Neither condition met

---

### 2. `src/reports/sanity_plots.py`

**Purpose:** Generate diagnostic plots and validation report

**Configuration:**
- Update the `FEATURE_CONFIG` dict once P2 completes

**Usage:**
```python
from src.reports.sanity_plots import generate_sanity_plots

generate_sanity_plots()
```

**Outputs:**

1. **regime_timeline.png** - SPX cumulative returns with regime background shading
   - Green: Risk-On periods
   - Red: Risk-Off periods
   - Grey: Neutral periods

2. **regime_stats.png** - Two-panel bar chart
   - Left: Mean daily return by regime
   - Right: Annualized volatility by regime

3. **f2_acceptance.md** - Markdown report with:
   - Dataset summary
   - Label distributions
   - Regime statistics
   - Validation checklist

---

## Running the Pipeline

### Option 1: Full Pipeline (Recommended)

```bash
cd /path/to/marketregime_hmm
uv run python src/p6_pipeline.py
```

This runs both `make_labels.py` and `sanity_plots.py` in sequence.

### Option 2: Individual Scripts

```bash
# Just generate labels
uv run python src/labels/make_labels.py

# Just generate plots
uv run python src/reports/sanity_plots.py
```

---

## Testing

Run unit tests:

```bash
uv run pytest tests/test_p6_labels.py -v
```

Test coverage:
- ✓ Volatility bucket tertile splitting
- ✓ Risk regime heuristic conditions
- ✓ Output file format and completeness
- ✓ No NaN values in labels
- ✓ End-to-end pipeline integration

---

## Configuration: Waiting for P2

The scripts are **fully parameterized** to work with any column names. When P2 completes:

1. **Check the actual column names** in `features_dataset.parquet`
2. **Update the config dicts:**

```python
# In make_labels.py
FEATURE_COLUMNS = {
    "volatility": "ACTUAL_VOL_COLUMN_NAME",
    "momentum_short": "ACTUAL_MOM_10_COLUMN_NAME",
    # ... etc
}

# In sanity_plots.py
FEATURE_CONFIG = {
    "spx_price": "ACTUAL_PRICE_COLUMN_NAME",
    "returns_1d": "ACTUAL_RETURNS_COLUMN_NAME",
}
```

3. **Run the pipeline** (no other code changes needed)

---

## Validation Checklist

Before considering P6 complete:

- [ ] `labels_prelim.csv` generated with no NaN values
- [ ] All dates covered (no gaps in index)
- [ ] Label distributions are reasonable (no single label dominates)
- [ ] **Risk-Off regimes show higher volatility** (sanity check)
- [ ] **Risk-On regimes show positive mean returns** (expected behavior)
- [ ] Plots render correctly and are saved to `reports/figures/`
- [ ] `f2_acceptance.md` report is generated
- [ ] Unit tests pass

---

## Expected Outputs

After successful run:

```
✅ TASK P6 COMPLETE - Ready for handoff to P7

Outputs:
  - data/processed/labels_prelim.csv
  - reports/figures/regime_timeline.png
  - reports/figures/regime_stats.png
  - data/processed/f2_acceptance.md
```

---

## Notes

- **Vectorized code:** No loops over rows (efficient for large datasets)
- **Error handling:** Clear messages if columns are missing or invalid
- **Modular design:** Functions can be imported and reused
- **Extensible:** Easy to add new regime definitions or plots

---

## Next Steps (P7+)

Once P6 outputs are validated, proceed to:
- **P7:** Model training (regime prediction)
- **P8:** Backtesting
- **P9:** Strategy optimization

---

**Status:** 🟡 WAITING FOR P2 COMPLETION

The skeleton is ready to go live as soon as P2 finishes.
