import os
import shutil

import pandas as pd

from etl.utils.connect_postgres import get_postgres_connection

SHAREPOINT_DIR = r"C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\productivity"


def earnedhrs_by_week_output():
    """Export int_castle__productivity_01_earnedhrs_by_week (org / operation / w-c week, complete weeks only)."""
    engine = get_postgres_connection()
    df = pd.read_sql(
        "SELECT * FROM analytics_intermediate.int_castle__productivity_01_earnedhrs_by_week "
        "ORDER BY week_commencing_date, org, operation_code",
        engine,
    )
    engine.dispose()

    last_complete_week = df["week_commencing_date"].max()
    last_complete_week_str = last_complete_week.strftime("%Y-%m-%d") if pd.notna(last_complete_week) else ""

    output_path = os.path.join("reports", "productivity", "earnedhrs_by_week.xlsx")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="earnedhrs_by_week", index=False, startrow=0, startcol=0)
        ws = writer.sheets["earnedhrs_by_week"]

        # Last complete week called out a couple columns to the right of the data
        note_col = len(df.columns) + 1
        ws.write(0, note_col, "Last complete week (w/c):")
        ws.write(0, note_col + 1, last_complete_week_str)

    print(f"Report written: {output_path} ({len(df)} rows)")

    os.makedirs(SHAREPOINT_DIR, exist_ok=True)
    shutil.copy2(output_path, os.path.join(SHAREPOINT_DIR, "earnedhrs_by_week.xlsx"))
    print(f"Copied to SharePoint: {SHAREPOINT_DIR}")
