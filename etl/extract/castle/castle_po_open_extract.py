import os

from config.paths import CASTLE_RAW_DIR
from etl.utils.read_csv import read_csv


def extract_castle_po_open():

    file = os.path.join(CASTLE_RAW_DIR, "PO OPEN.csv")

    df = read_csv(file, dtype=str)

    df.columns = (
        df.columns
        .str.replace("%", "pct", regex=False)
        .str.replace(" ", "_")
        .str.strip()
    )

    return df
