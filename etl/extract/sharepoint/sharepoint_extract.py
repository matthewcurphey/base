import os

import pandas as pd

from config.paths import SHAREPOINT_RAW_DIR

FILE_PATH = os.path.join(SHAREPOINT_RAW_DIR, "All open orders.xlsx")


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = (
        df.columns
        .str.replace("\xa0", " ", regex=False)
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    result = []
    for i, name in enumerate(cleaned):
        result.append(name if name else f"col_{i}")
    df.columns = result
    return df


def extract_open_orders() -> pd.DataFrame:
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(
            f"File not found: {FILE_PATH}\n"
            "Download 'All open orders.xlsx' from SharePoint and save it there."
        )

    xl = pd.ExcelFile(FILE_PATH, engine="openpyxl")

    # Build canonical column list from the first sheet (all headers present there)
    reference_cols = list(_clean_columns(xl.parse(xl.sheet_names[0])).columns)

    dfs = []
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name)
        df = _clean_columns(df)

        # Any col_N placeholder gets replaced with the reference name at that position
        df.columns = [
            reference_cols[i] if col.startswith("col_") and i < len(reference_cols) else col
            for i, col in enumerate(df.columns)
        ]

        df["source_tab"] = sheet_name
        try:
            df["snapshot_date"] = pd.to_datetime(sheet_name.strip(), format="%m.%d.%y")
        except ValueError:
            df["snapshot_date"] = pd.NaT

        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)
