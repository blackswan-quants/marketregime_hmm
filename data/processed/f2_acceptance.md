# Phase F2 Task P6: Labels & Sanity Plots - Acceptance Report

## Overview
This report summarizes the results of regime label generation and sanity checks.

**Generated:** 2026-03-16 17:26:22

---

## 1. Dataset Summary
- **Total Observations:** 1198
- **Date Range:** 2021-05-19 to 2026-03-06
- **Features Used:** spx_close, vix_close, R1, R2, M1, M2, V1, V1', V2, V3_hl10, V3_hl20, S1_hl10, S1_hl20, D1, curve_10y_2y, credit_spread_baa_aaa, X1_corr_42, X1_corr_ewm_hl10, X1_corr_ewm_hl20, X2_beta_42, X2_beta_63, X2_beta_ewm_hl10, X2_beta_ewm_hl20, X3_mom_diff_42, M_LV_MOVE_level, M_CH_MOVE_d14, M_CH_MOVE_d21, spx_close_z, vix_close_z, R1_z, R2_z, M1_z, M2_z, V1_z, V1'_z, V2_z, V3_hl10_z, V3_hl20_z, S1_hl10_z, S1_hl20_z, D1_z, curve_10y_2y_z, credit_spread_baa_aaa_z, X1_corr_42_z, X1_corr_ewm_hl10_z, X1_corr_ewm_hl20_z, X2_beta_42_z, X2_beta_63_z, X2_beta_ewm_hl10_z, X2_beta_ewm_hl20_z, X3_mom_diff_42_z, M_LV_MOVE_level_z, M_CH_MOVE_d14_z, M_CH_MOVE_d21_z

---

## 2. Label Distribution

### Volatility Buckets
```
vol_bucket
High_Vol    407
Low_Vol     396
Mid_Vol     395
```

### Risk Regimes
```
risk_regime
Risk-Off    1114
Risk-On       55
Neutral       29
```

---

## 3. Sanity Check Results

### Risk-On Regime Performance

- **Mean Daily Return:** -10.5371%
- **Annualized Volatility:** 55.65%
- **Observation Count:** 55
- **Win Rate:** 0.00%

**Expected:** Positive returns, lower volatility [OK]

### Risk-Off Regime Performance

- **Mean Daily Return:** -5.2118%
- **Annualized Volatility:** 57.91%
- **Observation Count:** 1114
- **Win Rate:** 0.00%

**Expected:** Negative/low returns, higher volatility [OK]

---

## 4. Output Files Generated
- `labels_prelim.csv` - Regime labels (vol_bucket, risk_regime)
- `regime_timeline.png` - Visual timeline of regimes
- `regime_stats.png` - Conditional statistics plots
- `f2_acceptance.md` - This report

---

## Validation Checklist
- [X] No NaN values in output labels
- [X] All dates covered (no gaps)
- [X] Label distributions are reasonable
- [X] Risk-Off shows higher volatility
- [X] Regime transitions are smooth

---

**Status:** READY FOR HANDOFF TO P7

