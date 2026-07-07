"""
One-off dev tool: dumps each new mcmaster mart into its own sheet in a
fresh workbook, separate from mcmaster_template.xlsx, so tab formatting
and layout can be designed against real data without touching the live
template. Re-run anytime to refresh with current data.
"""
import os

import pandas as pd
from openpyxl.styles import Font

from etl.utils.connect_postgres import get_postgres_connection

OUTPUT_PATH = os.path.join("reports", "mcmaster", "mcmaster_tabs_draft.xlsx")

MARTS = {
    "cross_ship": "mart_mcmaster__cross_ship",
    "hot_components": "mart_mcmaster__hot_components",
    "to_cancel": "mart_mcmaster__to_cancel",
    "dj_review": "mart_mcmaster__dj_review",
    "backlog_daily": "mart_mcmaster__backlog_daily",
    "backlog_status": "mart_mcmaster__backlog_status",
}


def dump_tabs_for_design():

    engine = get_postgres_connection()

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        for sheet_name, table_name in MARTS.items():
            df = pd.read_sql(f"SELECT * FROM analytics_marts.{table_name}", engine)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            ws = writer.sheets[sheet_name]
            for cell in ws[1]:
                cell.font = Font(bold=True)
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions

            for col_cells in ws.columns:
                length = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max(length + 2, 10), 40)

            print(f"{sheet_name}: {len(df)} rows")

    engine.dispose()
    print(f"\nWritten: {OUTPUT_PATH}")


if __name__ == "__main__":
    dump_tabs_for_design()
