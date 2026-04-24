# Market Regime Detector

A reproducible, production-grade framework for unsupervised market regime detection using **Gaussian Hidden Markov Models (HMM)** and **Dynamic Time Warping (DTW) clustering** on multi-asset financial data.

The system identifies latent market states — *Risk-On*, *Neutral*, and *Risk-Off* — from daily macroeconomic and cross-asset signals, providing interpretable regime labels that can inform systematic trading, risk management, and portfolio allocation decisions.

---

## Pipeline Walkthrough

For a full walkthrough of the pipeline — from raw data ingestion to validated regime labels, with figures and intermediate outputs at each stage — see the data report:

**[Market_Regime_Detector___Data_Report.pdf](Market_Regime_Detector___Data_Report.pdf)**

---

## Methodology Summary

```
Raw Data (FRED + Yahoo Finance)
        │
        ▼
Feature Engineering (market / credit / cross-asset)
        │
        ├── Stationarity tests (ADF, KPSS)
        ├── Collinearity filtering (VIF < 5)
        └── Per-category PCA → concatenated feature matrix
                │
                ▼
        Model Selection (penalized BIC, k=2..10)
                │         → optimal k=3
                ▼
        Gaussian HMM (50-start EM, diagonal covariance)
                │
                ├── Regime labels: Risk-On / Neutral / Risk-Off
                └── Posterior probabilities per timestamp
                        │
                        ▼
        Validation via DTW Hierarchical Clustering
                │
                └── ARI / NMI alignment metrics + confusion matrix
```

---

## Project Structure

```
marketregime_hmm/
├── src/
│   ├── model/
│   │   └── hmm_package/        # Core HMM: model selection, training, inference, visualization
│   ├── classes/
│   │   ├── metrics/            # Feature engineering (market, credit, cross-asset)
│   │   ├── clustering/         # DTW-based regime clustering
│   │   ├── data/               # Data fetching & alignment (FRED, Yahoo Finance)
│   │   └── viz/                # Visualization utilities
│   ├── reports/                # HMM vs DTW comparison reports
│   └── labels/                 # Heuristic regime label generation
├── data/
│   ├── raw/                    # Raw source data
│   ├── cleaned/                # Cleaned time series (parquet)
│   ├── processed/              # Standardized feature datasets (parquet)
│   └── reports/                # Generated figures and CSV reports
├── helpermodules/              # Statistical utilities (Granger causality, correlation)
└── pyproject.toml
```

---

## Key Outputs

| Output | Description |
|---|---|
| `hmm_model.pkl` | Trained HMM bundle (model, scaler, feature names, state labels) |
| `latest_dashboard.png` | 4-panel visualization: 3D feature scatter, regime heatmap, violin plots, SPX timeline |
| `model_selection.png` | AIC/BIC curves across k values |
| `financial_hypothesis_*.csv` | Per-regime statistics: returns, volatility, drawdown, curve/credit metrics |
| `distribution_stats.csv` | Skewness, kurtosis, ADF/KPSS p-values per feature |
| `vif_stats.csv` | Variance Inflation Factors for collinearity analysis |

---

## Setup

Requires Python 3.12+. Dependencies are managed with [UV](https://docs.astral.sh/uv/getting-started/installation/).

```sh
# Install dependencies and create virtual environment
uv sync
```

Activate the environment:

```sh
# macOS / Linux
source .venv/bin/activate

# Windows (PowerShell)
.\.venv\Scripts\Activate.ps1
```

Run the full pipeline:

```sh
python src/model/hmm_package/main.py
```

---

## Tech Stack

| Area | Libraries |
|---|---|
| HMM modeling | `hmmlearn` |
| DTW clustering | `dtaidistance`, `fastdtw` |
| Feature engineering | `pandas`, `numpy`, `statsmodels`, `scikit-learn` |
| Data ingestion | `yfinance`, `requests` (FRED), `pyarrow` |
| Visualization | `matplotlib`, `seaborn` |
| Code quality | `black`, `isort`, `flake8`, `pre-commit` |

---

## Code Quality

Pre-commit hooks enforce formatting and linting on every commit:

```sh
uv run pre-commit install        # one-time setup
uv run pre-commit run --all-files  # manual run
```

---

## License

MIT
