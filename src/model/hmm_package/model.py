
import numpy as np
import pandas as pd
from hmmlearn import hmm
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

def calculate_information_criteria(model, X):
    """
    Calculate AIC and BIC for the HMM.
    AIC = 2k - 2ln(L)
    BIC = k*ln(n) - 2ln(L)
    """
    try:
        log_likelihood = model.score(X)
    except:
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

def select_best_model(X_scaled, min_k=2, max_k=10, n_iter=200, n_init=10, random_state=42):
    """
    Find optimal number of regimes using AIC/BIC.
    """
    print(f"Comparing models with k={min_k}..{max_k} regimes...")
    results = []
    
    k_values = range(min_k, max_k + 1)
    
    for k in k_values:
        # Fit model with multiple initializations to avoid local optima
        try:
            model_k = hmm.GaussianHMM(n_components=k, covariance_type="full", n_iter=n_iter, random_state=random_state)
            model_k.fit(X_scaled)
            
            # Calculate metrics
            aic, bic, n_params, ll = calculate_information_criteria(model_k, X_scaled)
            results.append({
                'k': k,
                'AIC': aic,
                'BIC': bic,
                'LogLikelihood': ll,
                'Params': n_params
            })
            print(f"k={k:2d}: AIC={aic:10.1f}, BIC={bic:10.1f}, LogL={ll:10.1f}")
        except Exception as e:
            print(f"k={k}: Failed to fit ({str(e)})")
            
    df_results = pd.DataFrame(results)
    
    if not df_results.empty:
        # Sort by BIC and take the top one
        best_row = df_results.sort_values('BIC').iloc[0]
        best_k = int(best_row['k'])
        print(f"\nOptimal k by BIC: {best_k}")
        return best_k, df_results
    else:
        return min_k, df_results

def train_hmm(X_scaled, n_components, n_iter=200, random_state=42):
    """
    Train a single HMM model.
    """
    print(f"\nFitting Gaussian HMM with {n_components} regimes...")
    model = hmm.GaussianHMM(
        n_components=n_components,
        covariance_type="full",
        n_iter=n_iter,
        random_state=random_state
    )
    model.fit(X_scaled)
    
    logprob, Z_pred = model.decode(X_scaled)
    
    print(f"Model log-likelihood: {logprob:.2f}")
    print(f"Predicted regime distribution: {np.bincount(Z_pred)}")
    
    return model, Z_pred, logprob
