import os

from dotenv import load_dotenv

load_dotenv()

OBIEE_CONFIG = {
    "base_url": "http://obiprod.amcastle.com:9704",
    "user": os.environ["OBIEE_USER"],
    "password": os.environ["OBIEE_PASSWORD"],
}

# OBIEE catalog path -> raw filename in CASTLE_RAW_DIR (etl/extract/castle/*_extract.py
# read these directly). Only the daily-refreshed YTD reports belong here — the
# _2024/_2025 history files are static and aren't re-downloaded.
OBIEE_REPORTS = {
    "DJ": ("/users/mcurphey/Basic Report/Master/DJ", "DJ.csv"),
    "PO_OPEN": ("/users/mcurphey/Basic Report/Master/PO OPEN", "PO OPEN.csv"),
    "TRANSFERS": ("/users/mcurphey/Basic Report/Master/TRANSFERS", "TRANSFERS.csv"),
    "INVENTORY": ("/users/mcurphey/Basic Report/Master/INVENTORY", "INVENTORY.csv"),
    "PPS_RCV_SHP": ("/users/mcurphey/Basic Report/Master/PPS_RCV_SHP", "PPS_RCV_SHP.csv"),
    "SALES": ("/users/mcurphey/Basic Report/Master/SALES", "SALES.csv"),
    "PO_RECEIPTS": ("/users/mcurphey/Basic Report/Master/PO RECEIPTS", "PO RECEIPTS.csv"),
}
