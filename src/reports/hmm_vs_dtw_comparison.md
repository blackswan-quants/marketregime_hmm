# HMM vs DTW Clustering: Regime Comparison Report

**Generated:** 2026-04-14 17:27:44
**Date Range:** 2021-06-16 to 2026-03-10
**Overlapping Observations:** 1181

---

## 1. Agreement Metrics

| Metric | Value | Interpretation |
|--------|-------|----------------|
| Adjusted Rand Index | 0.4410 | Moderate agreement |
| Normalized Mutual Info | 0.3363 | Moderate shared information |

---

## 2. Confusion Matrix (HMM rows x DTW columns)

HMM Regime | DTW_Risk-Off | DTW_Risk-On
--- | --- | ---
Neutral | 208 | 69
Risk-Off | 187 | 92
Risk-On | 25 | 600

---

## 3. Per-Regime Statistics

### HMM Regimes

regime | mean_daily_return_pct | annualized_vol_pct | max_drawdown_pct | obs_count | win_rate_pct
--- | --- | --- | --- | --- | ---
Neutral | 0.2653248849103413 | 17.44066020616931 | -8.193757238146569 | 277 | 61.371841155234655
Risk-Off | -0.2305827682482368 | 25.529143646159802 | -53.01791501719546 | 279 | 42.29390681003584
Risk-On | 0.07014561895248021 | 11.028665358294289 | -5.19534301238301 | 625 | 56.48

### DTW Clusters

regime | mean_daily_return_pct | annualized_vol_pct | max_drawdown_pct | obs_count | win_rate_pct
--- | --- | --- | --- | --- | ---
Risk-Off | -0.006103267452361919 | 23.594815599593165 | -22.99065596514881 | 420 | 49.28571428571429
Risk-On | 0.07301811426307309 | 12.363503887473954 | -10.087607744393154 | 761 | 57.03022339027596

---

## 4. Financial Interpretation Validation

### HMM

method | regime | role | check | value | pass
--- | --- | --- | --- | --- | ---
HMM | Risk-On | Bull | positive mean return | 0.0701% | True
HMM | Risk-On | Bull | lowest volatility | 11.03% | True
HMM | Risk-Off | Bear | negative mean return | -0.2306% | True
HMM | Risk-Off | Bear | highest volatility | 25.53% | True

### DTW

method | regime | role | check | value | pass
--- | --- | --- | --- | --- | ---
DTW | Risk-On | Bull | positive mean return | 0.0730% | True
DTW | Risk-On | Bull | lowest volatility | 12.36% | True
DTW | Risk-Off | Bear | negative mean return | -0.0061% | True
DTW | Risk-Off | Bear | highest volatility | 23.59% | True

---

## 5. Figures

- `hmm_vs_dtw_regimes.png` — Side-by-side regime timelines on SPX price
- `regime_stats_comparison.png` — Per-regime return, volatility, drawdown bars
- `hmm_dtw_confusion_matrix.png` — Confusion matrix heatmap
