
import pandas as pd
import os

filepath = r"c:\Users\loren\PycharmProjects\marketregime_hmm\src\model\hmm_package\hmm_model_input.parquet"

if os.path.exists(filepath):
    df = pd.read_parquet(filepath)
    print("Columns in parquet file:")
    for col in df.columns:
        print(f"- {col}")
        
    pc_cols = [c for c in df.columns if 'PC' in c]
    print(f"\nPotential PCA columns found: {pc_cols}")
else:
    print(f"File not found: {filepath}")
