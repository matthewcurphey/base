import glob
import os

import pandas as pd

from config.paths import ORACLE_RAW_DIR

COLUMN_RENAME = {
    "Item Number":       "item_number",
    "Description":       "description",
    "Organization":      "organization",
    "Sourcing Rule":     "sourcing_rule",
    "Source Type":       "source_type",
    "Supplier":          "supplier",
    "Supplier Site":     "supplier_site",
    "Source Organization": "source_organization",
    "Intransit Time":    "intransit_time",
    "Make Buy":          "make_buy",
    "Purchasing Enabled": "purchasing_enabled",
    "Commodity":         "commodity",
    "Grade":             "grade",
    "Shape":             "shape",
    "Product Form":      "product_form",
    "Min Order Qty":     "min_order_qty",
    "Order Incrementals": "order_incrementals",
    "Preprocessing":     "preprocessing",
    "Lead Time":         "lead_time",
    "Postprocessing":    "postprocessing",
    "Fixed Days Supply": "fixed_days_supply",
    "Fixed Order Quantity": "fixed_order_quantity",
    "SS Method":         "ss_method",
    "Bucket Days":       "bucket_days",
    "Percent":           "percent",
    "Buyer":             "buyer",
    "Planner":           "planner",
    "Cu Stock Status":   "cu_stock_status",
    "Abc Identifier":    "abc_identifier",
    "Core Set":          "core_set",
    "XXX Intrastat":     "xxx_intrastat",
    "Cu Set":            "cu_set",
    "Container Spec":    "container_spec",
    "Spec Sourcing Rule": "spec_sourcing_rule",
    "Item Status":       "item_status",
    "Unit Of Measure":   "unit_of_measure",
}


def extract_castle_oracle_di() -> pd.DataFrame:
    pattern = os.path.join(ORACLE_RAW_DIR, "DI", "* DI.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No DI files found matching '* DI.csv' in {os.path.join(ORACLE_RAW_DIR, 'DI')}"
        )

    dfs = []
    for filepath in files:
        df = pd.read_csv(filepath, dtype=str, encoding="latin-1")
        df = df.rename(columns=COLUMN_RENAME)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)
