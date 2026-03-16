# HMM vs DTW Clustering: Regime Comparison Report

**Generated:** 2026-03-16 12:22:12
**Date Range:** 2025-03-06 to 2025-10-27
**Overlapping Observations:** 163

---

## 1. Agreement Metrics

| Metric | Value | Interpretation |
|--------|-------|----------------|
| Adjusted Rand Index | 0.4562 | Moderate agreement |
| Normalized Mutual Info | 0.3834 | Moderate shared information |

---

## 2. Confusion Matrix (HMM rows x DTW columns)

HMM Regime | DTW_1 | DTW_2 | DTW_3
--- | --- | --- | ---
Neutral | 7 | 19 | 5
Risk-Off | 14 | 0 | 18
Risk-On | 91 | 9 | 0

---

## 3. Per-Regime Statistics

### HMM Regimes

regime | mean_daily_return_pct | annualized_vol_pct | max_drawdown_pct | obs_count | win_rate_pct
--- | --- | --- | --- | --- | ---
Neutral | 0.18018235425450602 | 16.01356001722341 | -3.468336184394019 | 31 | 64.51612903225806
Risk-Off | -0.09609766531602341 | 43.90590357167291 | -12.453365842032515 | 32 | 53.125
Risk-On | 0.1450979831958858 | 10.214593407135963 | -3.022153153102729 | 100 | 56.99999999999999

### DTW Clusters

regime | mean_daily_return_pct | annualized_vol_pct | max_drawdown_pct | obs_count | win_rate_pct
--- | --- | --- | --- | --- | ---
1 | 0.048443037396542765 | 14.681735386395165 | -7.919567744066813 | 112 | 56.25
2 | 0.26998163094569544 | 14.701955416064685 | -2.663121639238877 | 28 | 64.28571428571429
3 | 0.17544435462927194 | 46.76796576356338 | -6.4999692679012995 | 23 | 56.52173913043478

---

## 4. Financial Interpretation Validation

### HMM

method | regime | role | check | value | pass
--- | --- | --- | --- | --- | ---
HMM | Risk-On | Bull | positive mean return | 0.1451% | True
HMM | Risk-On | Bull | lowest volatility | 10.21% | True
HMM | Risk-Off | Bear | negative mean return | -0.0961% | True
HMM | Risk-Off | Bear | highest volatility | 43.91% | True

### DTW

method | regime | role | check | value | pass
--- | --- | --- | --- | --- | ---
DTW | 1 | Bull | positive mean return | 0.0484% | True
DTW | 1 | Bull | lowest volatility | 14.68% | True
DTW | 3 | Bear | negative mean return | 0.1754% | False
DTW | 3 | Bear | highest volatility | 46.77% | True

---

## 5. Figures

- `hmm_vs_dtw_regimes.png` — Side-by-side regime timelines on SPX price
- `regime_stats_comparison.png` — Per-regime return, volatility, drawdown bars
- `hmm_dtw_confusion_matrix.png` — Confusion matrix heatmap
