import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.lower().strip() for col in df.columns]
    df = df.dropna()
    df = df.drop_duplicates()
    return df