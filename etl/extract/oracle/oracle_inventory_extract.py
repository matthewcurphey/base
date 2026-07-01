import glob
import os

import pandas as pd

from config.paths import ORACLE_RAW_DIR

COLUMNS = [
    "TMS1", "TMS2", "ALLOY", "TEMPER", "SPEC", "SF", "Alt_Length",
    "ActWHSE", "PriWHSE", "OnHandPCS", "OnHandFTorMT", "OnHandWeight",
    "OnHandValue", "LastReceiptDate", "MillPOVendor", "ActualMillPONumber",
    "FORM", "HEATPO", "CostPerLB", "WeightPerFTorMT",
    "Field21", "Field22", "Field23", "Field24", "Field25", "Field26", "Field27",
    "GenProduct", "Field29", "SubInv", "Locator", "THK",
    "Field33", "Field34", "Field35", "Length", "Width", "ITEM",
    "Field39", "Field40", "Field41", "Field42", "Field43", "Field44",
    "Field45", "Field46", "Field47", "Field48", "Field49", "AMC_Age",
    "LOTNo", "Field52", "Field53", "Field54", "DESC", "MILL",
]


def extract_castle_oracle_inventory() -> pd.DataFrame:
    pattern = os.path.join(ORACLE_RAW_DIR, "Inventory_*.txt")
    files = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No inventory files found matching Inventory_*.txt in {ORACLE_RAW_DIR}"
        )

    dfs = []
    for filepath in files:
        org = os.path.basename(filepath).replace("Inventory_", "").replace(".txt", "")
        df = pd.read_csv(filepath, header=None, names=COLUMNS, dtype=str, encoding="latin-1")
        df["org"] = org
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)
