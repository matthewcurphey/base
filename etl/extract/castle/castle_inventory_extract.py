import os
import pandas as pd

from config.paths import CASTLE_RAW_DIR
from etl.utils.read_csv import read_csv

def extract_castle_inventory():
    """
    Extracts and minimally cleans Castle sales data from raw CSV files.

    Returns:
        pd.DataFrame: Clean, combined sales dataset.
    """

    # File paths
    file  = os.path.join(CASTLE_RAW_DIR, "INVENTORY.csv")

    # Load CSVs with shared ingestion defaults
    df  = read_csv(file,  dtype=str)

    df.columns = (
        df.columns
        .str.replace("%", "pct", regex=False)
        .str.replace(" ", "_")
        .str.strip()
    )

    return df
