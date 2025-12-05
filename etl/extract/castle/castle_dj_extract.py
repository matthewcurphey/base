import os
import pandas as pd

from config.paths import CASTLE_RAW_DIR
from etl.utils.read_csv import read_csv

def extract_castle_dj():
    """
    Extracts and minimally cleans Castle sales data from raw CSV files.

    Returns:
        pd.DataFrame: Clean, combined sales dataset.
    """

    # File paths
    file_2024 = os.path.join(CASTLE_RAW_DIR, "DJ_2024.csv")
    file_ytd  = os.path.join(CASTLE_RAW_DIR, "DJ.csv")

    # Load CSVs with shared ingestion defaults
    df_2024 = read_csv(file_2024, dtype=str)
    df_ytd  = read_csv(file_ytd,  dtype=str)

    # Combine
    df = pd.concat([df_2024, df_ytd], ignore_index=True)

    df.columns = (
        df.columns
        .str.replace("%", "pct", regex=False)
        .str.replace(" ", "_")
        .str.strip()
    )

    return df
