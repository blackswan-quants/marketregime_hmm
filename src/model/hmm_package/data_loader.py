
import os
import numpy as np
import pandas as pd
from scipy.stats import skewnorm, t
from hmmlearn import hmm

def load_real_data(filepath):
    """
    Load real market data from parquet file.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
        
    df = pd.read_parquet(filepath)
    print(f"Loaded data columns: {df.columns.tolist()}")
    
    features = []
    feature_names = []
    
    # Check for PCA features first
    pc_cols = [c for c in df.columns if 'PC' in c]
    
    if len(pc_cols) > 0:
        print(f"Using PCA features: {pc_cols}")
        df = df.dropna(subset=pc_cols)
        for col in pc_cols:
            features.append(df[col].values)
            feature_names.append(col)
    else:
        # Fallback to original logic
        if 'spx_close' in df.columns:
            df['log_ret'] = np.log(df['spx_close'] / df['spx_close'].shift(1))
            df = df.dropna()
            features.append(df['log_ret'].values)
            feature_names.append("Log-Returns")
        elif 'R1' in df.columns:
            features.append(df['R1'].values)
            feature_names.append("Momentum (R1)")
            
        if 'V1' in df.columns:
            features.append(np.log(df['V1'].values + 1e-6))
            feature_names.append("Log-Volatility (V1)")
            
        if 'D1' in df.columns:
            features.append(df['D1'].values)
            feature_names.append("Drawdown (D1)")
            
        if 'M1' in df.columns:
            features.append(df['M1'].values)
            feature_names.append("Yield Curve (M1)")
        
    X = np.column_stack(features)
    
    # Create dummy Z_true since we don't have labels
    Z_true = np.zeros(len(X), dtype=int)
    
    return X, Z_true, feature_names

def generate_multifeature_data(n_samples=2000, noise_level=0.0):
    """
    Generate 4D observations with OVERLAPPING distributions (realistic/hard mode).
    """
    model = hmm.GaussianHMM(n_components=3, covariance_type="full")
    
    model.startprob_ = np.array([0.5, 0.2, 0.3])
    
    # Sticky transitions (realistic persistence)
    model.transmat_ = np.array([
        [0.96, 0.01, 0.03],  # Bull persists (~25 days avg)
        [0.02, 0.90, 0.08],  # Bear persists (~10 days avg)
        [0.02, 0.05, 0.93]   # Sideways persists (~14 days avg)
    ])
    
    # OVERLAPPING means (hard mode)
    # [Return, Log-Vol, Drawdown, Yield_Curve]
    model.means_ = np.array([
        [0.0002, -5.0, -0.02, 0.015],   # Bull
        [-0.0002, -4.0, -0.15, -0.005], # Bear (vol close to Bull!)
        [0.0000, -6.0, -0.05, 0.002],   # Sideways
    ])
    
    # Wide covariances (creates overlap)
    cov_bull = np.array([
        [0.00005, 0.0,    0.0,     0.0],
        [0.0,     0.30,   0.0,     0.0],
        [0.0,     0.0,    0.0005,  0.0],
        [0.0,     0.0,    0.0,     0.00001],
    ])
    
    cov_bear = np.array([
        [0.00025, -0.002, -0.0001, 0.00001],
        [-0.002,  0.80,   0.02,    -0.001],
        [-0.0001, 0.02,   0.002,   -0.0001],
        [0.00001, -0.001, -0.0001, 0.00002],
    ])
    
    cov_side = np.array([
        [0.00002, 0.0,    0.0,     0.0],
        [0.0,     0.15,   0.0,     0.0],
        [0.0,     0.0,    0.0003,  0.0],
        [0.0,     0.0,    0.0,     0.00001],
    ])
    
    model.covars_ = np.array([cov_bull, cov_bear, cov_side])
    
    X, Z_true = model.sample(n_samples)
    
    # Add measurement noise
    if noise_level > 0:
        feature_stds = X.std(axis=0)
        noise = np.random.randn(n_samples, 4) * feature_stds * noise_level
        X = X + noise
    
    feature_names = ["Log-Returns", "Log-Volatility", "Drawdown", "Yield Curve"]
    
    return X, Z_true, feature_names

def generate_broken_data(n_samples=2000):
    """
    Generates data that VIOLATES Gaussian assumptions (Fat Tails, Skewness).
    """
    # 1. Transition Matrix (Markovian dynamics remain)
    transmat = np.array([
        [0.95, 0.02, 0.03], # Bull
        [0.05, 0.85, 0.10], # Bear (less stable)
        [0.02, 0.03, 0.95]  # Sideways
    ])
    
    # Initial states
    start_prob = np.array([0.4, 0.2, 0.4])
    
    # Generate state sequence
    Z = np.zeros(n_samples, dtype=int)
    Z[0] = np.random.choice(3, p=start_prob)
    for i in range(1, n_samples):
        Z[i] = np.random.choice(3, p=transmat[Z[i-1]])
        
    X = np.zeros((n_samples, 4))
    
    # 2. Generate NON-GAUSSIAN Observations
    for i in range(n_samples):
        state = Z[i]
        
        if state == 0: # BULL: Skewed Positive
            # Skew-Normal: returns rise slowly (long right tail)
            r = skewnorm.rvs(a=4, loc=0.0005, scale=0.008) 
            v = np.random.normal(-5.0, 0.2)
            d = np.random.normal(-0.02, 0.01)
            y = np.random.normal(0.015, 0.002)
            
        elif state == 1: # BEAR: Fat Tails (Student-t)
            # t-Student with low df=2.5 generates massive outliers
            r = t.rvs(df=2.5, loc=-0.002, scale=0.015) 
            v = np.random.normal(-3.5, 0.8)
            d = np.random.normal(-0.15, 0.05)
            y = np.random.normal(-0.005, 0.005)
            
        elif state == 2: # SIDEWAYS: Gaussian (Control)
            r = np.random.normal(0, 0.005)
            v = np.random.normal(-6.0, 0.1)
            d = np.random.normal(-0.05, 0.01)
            y = np.random.normal(0.002, 0.001)
            
        X[i] = [r, v, d, y]

    feature_names = ["Log-Returns", "Log-Volatility", "Drawdown", "Yield Curve"]
    return X, Z, feature_names
