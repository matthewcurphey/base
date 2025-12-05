import os
import pandas as pd

NA_VALUES = ["Undefined", "N/A", "", "NULL", "error", "#DIV/0!"]

def read_csv(path, dtype=None):
    """
    Centralised CSV reader with consistent NA handling, stripping, and safety.
    """

    df = pd.read_csv(
        path,
        dtype=dtype,
        low_memory=False,
        na_values=NA_VALUES
    )
    
    # Clean column names globally
    df.columns = df.columns.str.strip()

    return df
