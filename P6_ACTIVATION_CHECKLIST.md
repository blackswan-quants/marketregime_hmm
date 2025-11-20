# P6 Activation Checklist (For When P2 Completes)

## Pre-Activation Steps

- [ ] P2 has completed and `data/processed/features_dataset.parquet` exists
- [ ] Open a terminal and navigate to the project root
- [ ] Activate the virtual environment: `. .venv/Scripts/Activate.ps1` (PowerShell)

## Step 1: Inspect P2 Output

```bash
uv run python -c "
import pandas as pd
df = pd.read_parquet('data/processed/features_dataset.parquet')
print('Shape:', df.shape)
print('\nColumns:')
for i, col in enumerate(df.columns):
    print(f'  {i+1}. {col}')
print('\nFirst row:')
print(df.iloc[0])
"
```

**Note the column names you see** - you'll need them in Step 2.

## Step 2: Update Configuration in `src/labels/make_labels.py`

Around **line 21**, update this section:

```python
FEATURE_COLUMNS = {
    # Volatility feature (for tertiles)
    "volatility": "[COLUMN_NAME_FROM_P2_STEP_1]",  # Should be a volatility measure
    
    # Momentum features
    "momentum_short": "[COLUMN_NAME_FROM_P2_STEP_1]",  # 10-day momentum
    "momentum_long": "[COLUMN_NAME_FROM_P2_STEP_1]",   # 42-day momentum
    
    # Risk indicators
    "vix_ewma_ratio": "[COLUMN_NAME_FROM_P2_STEP_1]",  # VIX / EWMA Vol
    "yc_slope": "[COLUMN_NAME_FROM_P2_STEP_1]",  # Yield curve slope
    "credit_spread_change": "[COLUMN_NAME_FROM_P2_STEP_1]",  # Credit spread change
    
    # Returns (for validation/reporting)
    "returns_1d": "[COLUMN_NAME_FROM_P2_STEP_1]",  # 1-day log returns
}
```

**Example** (if P2 output has columns named: spx_vol, r1, r2, v1, d1, etc.):
```python
FEATURE_COLUMNS = {
    "volatility": "v1",  # Assume V1 is volatility
    "momentum_short": "r1",  # Assume R1 is 10-day momentum
    "momentum_long": "r2",   # Assume R2 is 42-day momentum
    "vix_ewma_ratio": "v2",  # Assume V2 is VIX ratio
    "yc_slope": "m1",    # Assume M1 is yield curve slope
    "credit_spread_change": "m2",  # Assume M2 is credit spread
    "returns_1d": "spx_ret",  # Returns column
}
```

## Step 3: Update Configuration in `src/reports/sanity_plots.py`

Around **line 27**, update this section:

```python
FEATURE_CONFIG = {
    "spx_price": "[COLUMN_NAME_FROM_P2_STEP_1]",  # SPX close price
    "returns_1d": "[COLUMN_NAME_FROM_P2_STEP_1]",  # 1-day returns
}
```

**Example:**
```python
FEATURE_CONFIG = {
    "spx_price": "spx_close",  # Price column
    "returns_1d": "spx_ret",   # Returns column
}
```

## Step 4: Validate Configuration

Run the tests to ensure your column mappings are correct:

```bash
uv run pytest tests/test_p6_labels.py -v
```

Expected output: **8 passed**

## Step 5: Run the Full Pipeline

```bash
uv run python src/p6_pipeline.py
```

**Expected console output:**
```
======================================================================
STARTING TASK P6: LABELS & SANITY PLOTS
======================================================================

[STEP 1/2] Generating regime labels...
Loading features from data/processed/features_dataset.parquet...
Loaded XXX rows, XX columns
Computing volatility buckets...
Computing risk regimes...

LABEL SUMMARY STATISTICS
======================================================================
Volatility Bucket Distribution:
Low_Vol     XX
Mid_Vol     XX
High_Vol    XX

Risk Regime Distribution:
Risk-Off     XX
Risk-On      XX
Neutral      XX

Cross-tabulation:
...
✅ Label generation complete

[STEP 2/2] Generating sanity plots and report...
Loading features from data/processed/features_dataset.parquet...
Loading labels from data/processed/labels_prelim.csv...
Merged shape: (XXX, XX)

============================================================
GENERATING SANITY PLOTS
============================================================
Saving regime timeline to reports/figures/regime_timeline.png...
Saving conditional stats to reports/figures/regime_stats.png...

============================================================
REGIME STATISTICS SUMMARY
======================================================================
                        Mean Daily Return (%)  Annualized Volatility (%)  Observation Count
risk_regime                                                                                 
Risk-On                               [value]                    [value]                [XX]
Risk-Off                              [value]                    [value]                [XX]
Neutral                               [value]                    [value]                [XX]

Saving acceptance report to data/processed/f2_acceptance.md...

✅ All plots generated successfully!

======================================================================
✅ TASK P6 COMPLETE - Ready for handoff to P7
======================================================================

Outputs:
  - data/processed/labels_prelim.csv
  - reports/figures/regime_timeline.png
  - reports/figures/regime_stats.png
  - data/processed/f2_acceptance.md
```

## Step 6: Verify Output Files

```bash
# Check that all outputs exist:
Get-ChildItem data/processed/labels_prelim.csv
Get-ChildItem reports/figures/regime_timeline.png
Get-ChildItem reports/figures/regime_stats.png
Get-ChildItem data/processed/f2_acceptance.md

# Check the labels
uv run python -c "
import pandas as pd
df = pd.read_csv('data/processed/labels_prelim.csv', index_col=0)
print('Labels shape:', df.shape)
print('\nFirst 5 rows:')
print(df.head())
print('\nValue counts:')
print(df.value_counts())
"
```

## Step 7: Review the Acceptance Report

```bash
# On Windows PowerShell:
Get-Content data/processed/f2_acceptance.md

# Or open directly in VS Code:
# File -> Open -> data/processed/f2_acceptance.md
```

**Look for:**
- ✅ No NaN values
- ✅ Balanced label distribution (not all one label)
- ✅ Risk-Off shows higher volatility
- ✅ Risk-On shows positive/higher returns (typically)

## Step 8: Visual Inspection

Open the PNG plots in VS Code or your image viewer:

- `reports/figures/regime_timeline.png` - Should show price with colored background regions
- `reports/figures/regime_stats.png` - Should show two bar charts side-by-side

**Look for:**
- ✅ Smooth regime transitions (not wildly jumping)
- ✅ Red (Risk-Off) bars show higher volatility
- ✅ Green (Risk-On) bars show reasonable returns

## Troubleshooting

### Error: "Column 'XXX' not found in dataset"
→ Check your column names in FEATURE_COLUMNS/FEATURE_CONFIG dicts. Run Step 1 again to see actual column names.

### Error: "No NaN values in output labels"
→ The upstream data likely has NaNs. Ensure P2 handles them properly.

### Tests fail
→ Re-run Step 1 to inspect P2 output, then update config dicts carefully.

### Plots don't render
→ Check that `reports/figures/` directory exists (it should be auto-created, but verify)

---

## Success Criteria

- [x] P2 has completed
- [x] Configuration dicts updated with real column names
- [x] `uv run python src/p6_pipeline.py` runs without errors
- [x] All four output files are created
- [x] Labels CSV has no NaN values
- [x] Plots are visually reasonable
- [x] Acceptance report is generated

---

## Next: Proceed to P7

Once all checks pass:

```bash
# Commit your changes
git add src/labels/make_labels.py src/reports/sanity_plots.py data/processed/labels_prelim.csv
git commit -m "feat: P6 labels generation complete"

# Start P7 (model training)
# [P7 instructions will be provided]
```

---

**Estimated time to activate:** 10-15 minutes once P2 is done

**Questions?** Check `P6_README.md` for detailed documentation.
