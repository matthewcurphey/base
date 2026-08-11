import os
import shutil
from datetime import date

import pandas as pd

from etl.utils.connect_postgres import get_postgres_connection

ARCHIVE_DIR = os.path.join("reports", "yield", "archive")


def _export_mart(mart: str, sharepoint_dir: str, where: str = None):
    """
    Export a yield mart to CSV, copy it to SharePoint, and archive a dated
    copy. Shared save-local / copy-to-SharePoint / dated-archive pattern
    already used for the McMaster report. Pass `where` to filter rows
    (e.g. limit to a single day); omit it to export the mart as-is.
    """
    engine = get_postgres_connection()
    query = f"SELECT * FROM analytics_marts.{mart}"
    if where:
        query += f" WHERE {where}"
    df = pd.read_sql(query, engine)
    engine.dispose()

    output_path = os.path.join("reports", "yield", f"{mart}.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    # utf-8-sig adds a BOM so Excel recognizes the file as UTF-8 instead of
    # defaulting to Windows-1252, which mangles the op_ids/op_names arrows
    # (' → ') into 'â†’'.
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Report written: {output_path} ({len(df)} rows)")

    os.makedirs(sharepoint_dir, exist_ok=True)
    shutil.copy2(output_path, os.path.join(sharepoint_dir, f"{mart}.csv"))
    print(f"Copied to SharePoint: {sharepoint_dir}")

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    dated_name = f"{mart}_{date.today():%Y_%m_%d}.csv"
    archive_path = os.path.join(ARCHIVE_DIR, dated_name)
    shutil.copy2(output_path, archive_path)
    print(f"Archived: {archive_path}")


def yield_output():
    """Export mart_yield_siteopdate — the source file the Yield Loss Power BI dataset refreshes from."""
    _export_mart(
        "mart_yield_siteopdate",
        r"C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\etl_dumps",
    )


def yield_mart_output():
    """Export mart_yield_output — the trimmed job-level mart, dropped in yield's old SharePoint slot.

    Limited to yesterday's completed jobs (complete_date = current_date - 1).
    """
    _export_mart(
        "mart_yield_output",
        r"C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\yield",
        where="complete_date = current_date - 1",
    )
