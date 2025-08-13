import pandas as pd

def clean_chunk(df: pd.DataFrame, req_cols, date_cols):
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    for c in date_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    for c in ["quantity", "unit_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["amount"] = df["quantity"] * df["unit_price"]

    df = df.dropna(subset=req_cols)
    df = df[df["quantity"] > 0]
    df = df[df["unit_price"] >= 0]
    return df
