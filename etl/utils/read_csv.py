import os
import pandas as pd

NA_VALUES = ["Undefined", "N/A", "", "NULL", "error", "#DIV/0!"]

def read_csv(path, dtype=None, encoding="utf-8", skiprows=None):
    """
    Centralised CSV reader with consistent NA handling, stripping, and safety.
    Pass encoding="ISO-8859-1" for files containing non-UTF-8 characters (e.g. French/Spanish names).
    Pass skiprows=N to skip N rows before treating the next row as the header.
    """

    df = pd.read_csv(
        path,
        dtype=dtype,
        low_memory=False,
        na_values=NA_VALUES,
        encoding=encoding,
        skiprows=skiprows,
    )
    
    # Clean column names globally
    df.columns = df.columns.str.strip()

    return df
