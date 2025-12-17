import logging

import numpy as np
import pandas as pd

from .model import HMMConfig, _get_regime_durations, train_hmm

logger = logging.getLogger(__name__)


def run_rolling_analysis(
    X_scaled: np.ndarray,
    timestamps: pd.DatetimeIndex,
    n_components: int,
    hmm_config: HMMConfig,
    window_size: int = 100,
    step_size: int = 21,
) -> pd.DataFrame:
    """
    Performs rolling window HMM training to evaluate model stability and track transition matrix drift.

    Args:
        X_scaled: The full, scaled historical feature data.
        timestamps: Datetime index corresponding to X_scaled.
        n_components: The final chosen number of regimes (k).
        hmm_config: HMM configuration object.
        window_size: The size of the training window (in days/observations).
        step_size: The number of observations to advance the window each step.

    Returns:
        A DataFrame with the window end date as index and the collected metrics.
    """
    if timestamps is None:
        raise ValueError("timestamps is None.")

    timestamps = pd.DatetimeIndex(pd.to_datetime(timestamps))

    # Align lengths
    n = min(len(X_scaled), len(timestamps))
    if n == 0:
        raise ValueError("Empty input: X_scaled or timestamps has zero length.")

    if len(X_scaled) != len(timestamps):
        logger.warning(
            "Length mismatch: len(X_scaled)=%d, len(timestamps)=%d. Slicing both to %d.",
            len(X_scaled),
            len(timestamps),
            n,
        )
        X_scaled = X_scaled[:n]
        timestamps = timestamps[:n]

    if n < window_size:
        logger.error("Not enough samples for rolling: n=%d < window_size=%d", n, window_size)
        return pd.DataFrame()

    logger.info("Starting Rolling Window Analysis (Window=%d, Step=%d)", window_size, step_size)

    results = []
    start_idx, end_idx = 0, window_size

    while end_idx <= n:
        X_window = X_scaled[start_idx:end_idx]
        window_end_date = timestamps[end_idx - 1]

        try:
            model, Z_pred_window, logprob = train_hmm(X_window, n_components, hmm_config)

            transmat = model.transmat_
            durations = _get_regime_durations(transmat)

            metrics = {
                "LogLikelihood": float(logprob),
                "MinDuration": float(durations.min()),
                "WindowEndDate": window_end_date,
            }

            for i in range(n_components):
                for j in range(n_components):
                    metrics[f"A_{i+1}{j+1}"] = float(transmat[i, j])
                metrics[f"Dur_{i+1}"] = float(durations[i])

            results.append(metrics)

        except Exception as e:
            logger.warning(
                "Rolling fit failed for window ending %s: %s", window_end_date.strftime("%Y-%m-%d"), e, exc_info=True
            )

        start_idx += step_size
        end_idx += step_size

    if not results:
        logger.error("No successful HMM fits during rolling analysis (results is empty). Check training logs.")
        return pd.DataFrame()

    df_rolling = pd.DataFrame(results).set_index("WindowEndDate")
    logger.info("Rolling analysis complete. %d windows processed.", len(df_rolling))
    return df_rolling
