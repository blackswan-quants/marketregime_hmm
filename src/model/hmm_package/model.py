import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from hmmlearn import hmm

logger = logging.getLogger(__name__)


@dataclass
class HMMConfig:
    """Configuration for HMM training."""

    n_iter: int = 200
    n_init: int = 50
    covariance_type: str = "diag"
    min_duration_penalty_threshold: float = 5.0
    duration_penalty: float = 10000.0
    tol: float = 1e-2
    random_state: Optional[int] = None
    persistence_weight: float = 0.95  # Probability of staying in the same state
    bic_penalty_multiplier: float = 1.0
    aic_penalty_multiplier: float = 1.0
    k_preference: Dict[int, float] = field(default_factory=dict)


def calculate_information_criteria(
    model, X: np.ndarray, bic_multiplier: float = 1.0, aic_multiplier: float = 1.0
) -> Tuple[float, float, float, float]:
    """Calculate AIC and BIC for the HMM."""
    try:
        log_likelihood = model.score(X)
    except Exception as e:
        logger.warning("Failed to score model: %s", e)
        return np.inf, np.inf, 0, -np.inf

    n_samples = len(X)
    n_features = model.n_features
    n_components = model.n_components

    n_startprob = n_components - 1
    n_transmat = n_components * (n_components - 1)
    n_means = n_components * n_features

    cov_type = getattr(model, "covariance_type", "full")
    cov_params = {
        "full": n_components * n_features * (n_features + 1) // 2,
        "diag": n_components * n_features,
        "spherical": n_components,
        "tied": n_features * (n_features + 1) // 2,
    }
    n_covars = cov_params.get(cov_type, cov_params["full"])

    k = n_startprob + n_transmat + n_means + n_covars
    aic = (2 * k * aic_multiplier) - 2 * log_likelihood
    bic = (k * np.log(n_samples) * bic_multiplier) - 2 * log_likelihood

    return aic, bic, k, log_likelihood


def _fit_single_hmm(X: np.ndarray, n_components: int, config: HMMConfig, seed_offset: int = 0):
    """Fit a single HMM with given random state offset. Returns (model, score) or (None, -inf)."""
    rs = config.random_state + seed_offset if config.random_state is not None else None
    try:
        model = hmm.GaussianHMM(
            n_components=n_components,
            covariance_type=config.covariance_type,
            n_iter=config.n_iter,
            tol=config.tol,
            random_state=rs,
        )

        # Initialize transition matrix with strong diagonal if persistence_weight is set.
        # Remove "t" from init_params so hmmlearn doesn't overwrite our transmat_ during fit().
        if config.persistence_weight > 0:
            transmat = np.full((n_components, n_components), (1 - config.persistence_weight) / (n_components - 1))
            np.fill_diagonal(transmat, config.persistence_weight)
            model.transmat_ = transmat
            model.init_params = model.init_params.replace("t", "")

        model.fit(X)

        if not model.monitor_.converged:
            logger.debug("Model (rs=%s) did not converge", rs)

        return model, model.score(X)
    except Exception as e:
        logger.debug("Fit failed (rs=%s): %s", rs, e)
        return None, -np.inf


def _get_regime_durations(transmat: np.ndarray) -> np.ndarray:
    """Calculate expected regime durations from transition matrix diagonal."""
    diag = np.diag(transmat)
    # Clamp to avoid division issues when self-transition prob is 1.0
    diag_clamped = np.clip(diag, 0, 0.9999)
    return 1.0 / (1.0 - diag_clamped)


def select_best_model(
    X_scaled: np.ndarray,
    min_k: int = 2,
    max_k: int = 10,
    config: Optional[HMMConfig] = None,
) -> Tuple[int, pd.DataFrame]:
    """Find optimal number of regimes using penalized BIC."""
    config = config or HMMConfig()
    logger.info("Comparing models k=%d..%d (%d inits, %s cov)", min_k, max_k, config.n_init, config.covariance_type)

    results = []

    for k in range(min_k, max_k + 1):
        best_model, best_ll = None, -np.inf

        for i in range(config.n_init):
            model, score = _fit_single_hmm(X_scaled, k, config, seed_offset=i)
            if model is not None and score > best_ll:
                best_model, best_ll = model, score

        if best_model is None:
            logger.warning("k=%d: all fits failed", k)
            continue

        aic, bic, n_params, ll = calculate_information_criteria(
            best_model, X_scaled, config.bic_penalty_multiplier, config.aic_penalty_multiplier
        )
        durations = _get_regime_durations(best_model.transmat_)
        min_duration = durations.min()

        penalty_applied = min_duration < config.min_duration_penalty_threshold

        bias = config.k_preference.get(k, 0.0)
        final_bic = bic + (config.duration_penalty if penalty_applied else 0) - bias

        results.append(
            {
                "k": k,
                "AIC": aic,
                "BIC": final_bic,
                "RawBIC": bic,
                "LogLikelihood": ll,
                "MinDuration": min_duration,
                "Converged": best_model.monitor_.converged,
            }
        )

        dur_str = ", ".join(f"{d:.1f}d" for d in durations)
        logger.info(
            "k=%2d: BIC=%10.1f%s, LogL=%10.1f | Durs: [%s]",
            k,
            final_bic,
            " [PEN]" if penalty_applied else "",
            ll,
            dur_str,
        )

    df = pd.DataFrame(results)
    if df.empty:
        return min_k, df

    best_k = int(df.sort_values("BIC").iloc[0]["k"])
    logger.info("Optimal k by penalized BIC: %d", best_k)
    return best_k, df


def infer_state_labels(model: hmm.GaussianHMM, feature_names: list) -> dict:
    """
    Infer semantic regime labels based on model means.

    Args:
        model: Trained HMM model
        feature_names: List of feature names

    Returns:
        Dictionary mapping state indices to descriptive labels
    """
    n_states = model.n_components
    covars = model.covars_

    # Heuristic: Volatility is a better indicator of regime than returns.
    # We use the trace of the covariance matrix as a proxy for total variance (volatility) in that state
    state_vols = []
    for i in range(n_states):
        if model.covariance_type == "full":
            state_vols.append(np.trace(covars[i]))
        elif model.covariance_type == "diag":
            # Diag covariance is just an array of variances
            state_vols.append(np.sum(covars[i]))
        else:
            # Fallback
            state_vols.append(np.sum(covars[i]))

    sorted_indices = np.argsort(state_vols)

    # Map states to low/medium/high based on Volatility (Trace of Covariance)
    labels = {}
    if n_states == 2:
        labels[sorted_indices[0]] = "Risk-On"
        labels[sorted_indices[1]] = "Risk-Off"
    elif n_states == 3:
        labels[sorted_indices[0]] = "Risk-On"
        labels[sorted_indices[1]] = "Neutral"
        labels[sorted_indices[2]] = "Risk-Off"
    else:
        # For more states, use numeric labels sorted by vol
        for i, idx in enumerate(sorted_indices):
            labels[idx] = f"Regime_Vol_{i}"

    return labels


def train_hmm(
    X_scaled: np.ndarray,
    n_components: int,
    config: Optional[HMMConfig] = None,
) -> Tuple[hmm.GaussianHMM, np.ndarray, float]:
    """Train a single HMM model with multi-start."""
    config = config or HMMConfig()
    logger.info("Fitting GaussianHMM with %d regimes (%d inits)", n_components, config.n_init)

    best_model, best_ll = None, -np.inf
    for i in range(config.n_init):
        model, score = _fit_single_hmm(X_scaled, n_components, config, seed_offset=i)
        if model is not None and score > best_ll:
            best_model, best_ll = model, score

    if best_model is None:
        raise ValueError("All HMM fits failed")

    logprob, Z_pred = best_model.decode(X_scaled)

    logger.info("Log-likelihood: %.2f | Converged: %s", logprob, best_model.monitor_.converged)
    logger.info("Regime counts: %s", np.bincount(Z_pred))
    logger.info("Transition diag (persistence): %s", np.diag(best_model.transmat_).round(3))

    return best_model, Z_pred, logprob
