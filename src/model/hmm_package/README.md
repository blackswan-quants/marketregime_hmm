
# HMM Market Regime Detection Package

This package refactors the original notebook logic into modular Python scripts for better maintainability and reusability.

## Structure

- `config.py`: Configuration parameters (data type, noise level, regime names/colors).
- `data_loader.py`: Functions to load Real/Gaussian/Broken data, including PCA feature handling.
- `model.py`: Core HMM logic (Model Selection with AIC/BIC, Training, Decoding).
- `vis.py`: Visualization functions (Dashboard, Model Selection plots).
- `main.py`: Entry point to run the full pipeline.

## Usage

To run the pipeline using the default configuration (`DATA_TYPE="Real"`), execute:

```bash
python -m src.model.hmm_package.main
```

Ensure `hmm_model_input.parquet` is in the root directory (or update `REAL_DATA_PATH` in `config.py`).
If the real data file is missing, the pipeline will fall back to Synthetic data automatically.
