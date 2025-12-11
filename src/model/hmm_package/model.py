import numpy as np
import pandas as pd
from hmmlearn import hmm


def calculate_information_criteria(model, X):
    """
    Calculate AIC and BIC for the HMM.
    AIC = 2k - 2ln(L)
    BIC = k*ln(n) - 2ln(L)
    """
    try:
        log_likelihood = model.score(X)
    except Exception:
        return np.inf, np.inf, 0, -np.inf

    n_samples = len(X)

    # Calculate number of parameters
    n_features = model.n_features
    n_components = model.n_components

    # 1. Start probabilities: n_components - 1
    n_startprob = n_components - 1

    # 2. Transition matrix: n_components * (n_components - 1)
    n_transmat = n_components * (n_components - 1)

    # 3. Means: n_components * n_features
    n_means = n_components * n_features

    # 4. Covariances (full): n_components * n_features * (n_features + 1) / 2
    n_covars = n_components * n_features * (n_features + 1) / 2

    k = n_startprob + n_transmat + n_means + n_covars

    aic = 2 * k - 2 * log_likelihood
    bic = k * np.log(n_samples) - 2 * log_likelihood

    return aic, bic, k, log_likelihood


def select_best_model(X_scaled, min_k=2, max_k=10, n_iter=200, n_init=50, random_state=None):
    """
    Find optimal number of regimes using AIC/BIC with robust multi-start fitting.
    Applies penalties for short duration regimes.
    """
    print(f"Comparing models with k={min_k}..{max_k} regimes (Robust: {n_init} starts, diag cov)...")
    results = []

    k_values = range(min_k, max_k + 1)

    for k in k_values:
        best_model_k = None
        best_ll = -np.inf

        # 1. Multi-start loop
        for i in range(n_init):
            try:
                # Use 'diag' covariance to reduce parameters and overfitting
                # Use different random state for each start if not fixed
                rs = random_state + i if random_state is not None else None

                model_cand = hmm.GaussianHMM(n_components=k, covariance_type="diag", n_iter=n_iter, random_state=rs)
                model_cand.fit(X_scaled)

                if model_cand.score(X_scaled) > best_ll:
                    best_ll = model_cand.score(X_scaled)
                    best_model_k = model_cand
            except Exception:
                continue

        if best_model_k is None:
            print(f"k={k}: Failed to fit any model.")
            continue

        # 2. Calculate Metrics
        aic, bic, n_params, ll = calculate_information_criteria(best_model_k, X_scaled)

        # 3. Time Duration Logic & Penalty
        # Avg duration = 1 / (1 - self_trans)
        diag_trans = np.diag(best_model_k.transmat_)
        # Handle 1.0 case to avoid divide by zero (infinite duration)
        with np.errstate(divide="ignore"):
            durations = 1.0 / (1.0 - diag_trans + 1e-6)

        min_duration = np.min(durations)
        avg_duration_str = ", ".join([f"{d:.1f}d" for d in durations])

        # Penalize if any regime lasts less than 5 days average
        penalty_msg = ""
        final_bic = bic

        if min_duration < 5.0:
            final_bic += 10000  # Large penalty
            penalty_msg = " [PENALIZED < 5d]"

        results.append(
            {
                "k": k,
                "AIC": aic,
                "BIC": final_bic,  # Storing penalised BIC for selection
                "RawBIC": bic,  # Keep raw for reference
                "LogLikelihood": ll,
                "MinDuration": min_duration,
            }
        )

        print(
            f"k={k:2d}: BIC={final_bic:10.1f} (Raw: {bic:10.1f}), "
            f"LogL={ll:10.1f} | Durs: [{avg_duration_str}]{penalty_msg}"
        )

    df_results = pd.DataFrame(results)

    if not df_results.empty:
        # Sort by Penalized BIC and take the top one
        best_row = df_results.sort_values("BIC").iloc[0]
        best_k = int(best_row["k"])
        print(f"\nOptimal k by Penalized BIC: {best_k}")
        return best_k, df_results
    else:
        return min_k, df_results


def train_hmm(X_scaled, n_components, n_iter=200, random_state=None):
    """
    Train a single HMM model (Uses robust fitting similar to selection).
    """
    print(f"\nFitting Gaussian HMM with {n_components} regimes (Robust 'diag')...")

    best_model = None
    best_ll = -np.inf
    n_init = 20  # Retry a few times to get a good fit

    for i in range(n_init):
        try:
            rs = random_state + i if random_state is not None else None
            model = hmm.GaussianHMM(n_components=n_components, covariance_type="diag", n_iter=n_iter, random_state=rs)
            model.fit(X_scaled)
            score = model.score(X_scaled)
            if score > best_ll:
                best_ll = score
                best_model = model
        except Exception:
            continue

    if best_model is None:
        raise ValueError("Failed to fit model.")

    logprob, Z_pred = best_model.decode(X_scaled)

    print(f"Model log-likelihood: {logprob:.2f}")
    print(f"Predicted regime distribution: {np.bincount(Z_pred)}")

    # Print Transmat diagonal for user verification
    print("\nTransition Matrix Diagonal (Persistence):")
    print(np.diag(best_model.transmat_))

    return best_model, Z_pred, logprob
