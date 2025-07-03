import pandas as pd

def load_to_parquet(df: pd.DataFrame, path_output: str):
    df.to_parquet(path_output, index=False)
