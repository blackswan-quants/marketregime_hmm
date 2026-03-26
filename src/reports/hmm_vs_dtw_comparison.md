# HMM vs DTW Clustering: Regime Comparison Report

**Generated:** 2026-03-25 08:44:05
**Date Range:** 2021-06-17 to 2026-03-06
**Overlapping Observations:** 1178

---

## 1. Agreement Metrics

| Metric | Value | Interpretation |
|--------|-------|----------------|
| Adjusted Rand Index | 0.3400 | Moderate agreement |
| Normalized Mutual Info | 0.2203 | Low shared information |

---

## 2. Confusion Matrix (HMM rows x DTW columns)

HMM Regime | DTW_Neutral | DTW_Risk-Off | DTW_Risk-On
--- | --- | --- | ---
Neutral | 14 | 104 | 88
Risk-Off | 13 | 78 | 154
Risk-On | 0 | 18 | 709

---

## 3. Per-Regime Statistics

### HMM Regimes

regime | mean_daily_return_pct | annualized_vol_pct | max_drawdown_pct | obs_count | win_rate_pct
--- | --- | --- | --- | --- | ---
Neutral | 0.20006268687263168 | 17.56264176818979 | -4.870302095345953 | 206 | 56.79611650485437
Risk-Off | -0.15238279604453042 | 27.38828800886272 | -39.095603525433724 | 245 | 45.30612244897959
Risk-On | 0.06772163096154989 | 11.708897780671142 | -6.314200628287833 | 727 | 56.671251719394775

### DTW Clusters

regime | mean_daily_return_pct | annualized_vol_pct | max_drawdown_pct | obs_count | win_rate_pct
--- | --- | --- | --- | --- | ---
Neutral | 0.46559762126987503 | 19.548355289650978 | -4.734711616777982 | 27 | 74.07407407407408
Risk-Off | 0.056517409140399676 | 22.443390866588388 | -12.370313050295234 | 200 | 46.5
Risk-On | 0.030744623103609228 | 15.83167464193764 | -24.95542076409153 | 951 | 55.41535226077813

---

## 4. Financial Interpretation Validation

### HMM

method | regime | role | check | value | pass
--- | --- | --- | --- | --- | ---
HMM | Risk-On | Bull | positive mean return | 0.0677% | True
HMM | Risk-On | Bull | lowest volatility | 11.71% | True
HMM | Risk-Off | Bear | negative mean return | -0.1524% | True
HMM | Risk-Off | Bear | highest volatility | 27.39% | True

### DTW

method | regime | role | check | value | pass
--- | --- | --- | --- | --- | ---
DTW | Risk-On | Bull | positive mean return | 0.0307% | True
DTW | Risk-On | Bull | lowest volatility | 15.83% | True
DTW | Risk-Off | Bear | negative mean return | 0.0565% | False
DTW | Risk-Off | Bear | highest volatility | 22.44% | True

---

## 5. Figures

- `hmm_vs_dtw_regimes.png` — Side-by-side regime timelines on SPX price
- `regime_stats_comparison.png` — Per-regime return, volatility, drawdown bars
- `hmm_dtw_confusion_matrix.png` — Confusion matrix heatmap
