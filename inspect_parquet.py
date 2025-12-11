import os

import pandas as pd

filepath = r"c:\Users\loren\Documents\VSCode projects\marketregime_hmm\src\model\hmm_package\hmm_model_input.parquet"
if os.path.exists(filepath):
    df = pd.read_parquet(filepath)
    print("CTX_COL_START")
    for c in df.columns:
        print(c)
    print("CTX_COL_END")
else:
    print("File not found")
