import os
import shutil
from datetime import date

import pandas as pd

from etl.utils.connect_postgres import get_postgres_connection

MART = "mart_yield_siteopdate"

OUTPUT_PATH = os.path.join("reports", "yield", f"{MART}.csv")
ARCHIVE_DIR = os.path.join("reports", "yield", "archive")
SHAREPOINT_DIR = r"C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\yield"


def yield_output():
    """
    Export mart_yield_siteopdate as-is (no filtering) to CSV — the source
    file the Yield Loss Power BI dataset refreshes from. Replaces the
    manual "export from Postgres, drop it in SharePoint" step with the
    same save-local / copy-to-SharePoint / dated-archive pattern already
    used for the McMaster report.
    """
    engine = get_postgres_connection()
    df = pd.read_sql(f"SELECT * FROM analytics_marts.{MART}", engine)
    engine.dispose()

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Report written: {OUTPUT_PATH} ({len(df)} rows)")

    os.makedirs(SHAREPOINT_DIR, exist_ok=True)
    shutil.copy2(OUTPUT_PATH, os.path.join(SHAREPOINT_DIR, f"{MART}.csv"))
    print(f"Copied to SharePoint: {SHAREPOINT_DIR}")

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    dated_name = f"{MART}_{date.today():%Y_%m_%d}.csv"
    archive_path = os.path.join(ARCHIVE_DIR, dated_name)
    shutil.copy2(OUTPUT_PATH, archive_path)
    print(f"Archived: {archive_path}")
