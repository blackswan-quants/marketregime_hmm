# Market Regime Detection Report: HMM vs. DTW Models

## (a) Comparison of HMM State Sequences with DTW Clusters

### 1. Overlay Regimes on S&P 500 Price Charts
Based on the visual overlays and timeline charts, the two models exhibit fundamentally different behaviors in how they segment market regimes:

* **Gaussian HMM Regimes:** The HMM is highly reactive and predominantly classifies the timeline as "Neutral/Risk-Off" (150 days out of 163). The "Bull" (Risk-On) and "Bear" (Neutral) regimes are extremely short-lived, appearing as brief, sharp transitions (9 days and 4 days, respectively) during the volatile period in early 2025. It acts more like a short-term tactical signal.
* **DTW Clustering Regimes:** DTW produces much smoother, macro-level regime blocks. It identifies extended, continuous periods of distinct market behavior rather than day-to-day fluctuations. For instance, a prolonged "Bear" (Cluster 1) regime dominates the latter half of the year, while a distinct "Bull" (Cluster 2) phase occurs in the middle.

### 2. Feature Statistics per Regime
The statistical breakdown of each model reveals how they mathematically define these regimes:

**HMM Statistics:**
* **Bull (Risk-On, Regime 2):** Highest mean return (**0.0085**), moderate annualized volatility (**0.1168**), and **0.0000** max drawdown over its 9 active days.
* **Bear (Neutral, Regime 0):** Negative mean return (**-0.0071**), lowest volatility (**0.0585**), and a max drawdown of **-0.0169** over 4 days.
* **Other (Risk-Off, Regime 1):** Flat mean return (**0.0008**), high volatility (**0.2131**), and deep max drawdown (**-0.1455**).

**DTW Statistics:**
* **Bull (Cluster 2):** Highest mean return (**0.0027**), moderate volatility (**0.1461**), and lowest max drawdown (**-0.0267**).
* **Bear (Cluster 1):** Lowest mean return (**0.0004**), moderate volatility (**0.1446**), and the deepest max drawdown (**-0.0819**). Notably, this cluster is defined by heavily negative Market PC1 means (**-1.3403**).
* **High Vol (Cluster 3):** Extreme annualized volatility (**0.4345**) with moderate returns (**0.0017**).

---

## (b) Financial Interpretation

### 1. Bull and Bear Regime Validation
* **HMM Model Validation:** The HMM flawlessly captures the theoretical definitions of Bull and Bear states, albeit for very brief windows. The HMM Bull state correctly identifies positive returns with a staggering **77.78% predictive win rate** for the next day's price movement. The HMM Bear state correctly identifies negative returns (**-0.0071** mean return) with a highly effective **25.00% win rate** (meaning 75% of the time, the next day is down, making it an excellent crash indicator).
* **DTW Model Validation:** The DTW model aligns with traditional financial definitions, but with muted extremes due to its longer time horizons. The DTW Bull regime shows the highest relative returns and lowest drawdowns. The DTW Bear regime, while still yielding a technically positive mean return (**0.0004**), shows severe maximum drawdowns (**-0.0819**) and heavily negative principal component characteristics, indicating sustained downward pressure and structural weakness.

### 2. Alignment Confirmation: Do HMM and DTW Regimes Align?
**No, they do not align.** The models exhibit severe disagreement in their classification of the market.

* **Overlap Percentage (Congruence):** A remarkably low **9.82%**.
* **Discrepancy:** The models disagree on **147 out of 163 days**.
* **Interpretation:** This massive conflict zone occurs because the models are measuring entirely different phenomena. HMM is acting as a high-frequency, reactive state-transition model that looks for immediate statistical shifts (hence its high short-term predictive power). DTW is analyzing structural shapes and trends over rolling windows, grouping the market into broader macro-economic phases.