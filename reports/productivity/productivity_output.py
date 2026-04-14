import os
import pandas as pd
from etl.utils.connect_postgres import get_postgres_connection
from reports.productivity.branch_config import BRANCHES


DEFAULT_ORGS = ('ASC','ATL','CLE','DAL','ENA','ENT','HAI','JVL','LOS','MCH','MTY','MXM','MXQ','SGP','STO','TOR','WIE')
#DEFAULT_ORGS = ('ASC','ATL','CLE','DAL','ENA','ENT','HAI','JVL','LOS','MCH','MTY','MXM','MXQ','SGP','STO','TOR','WIE')


def productivity_output(output_year: int, output_month: int, orgs: tuple = DEFAULT_ORGS):

    engine = get_postgres_connection()

    # ------------------------------------------------------------------
    # 1. Load dbt tables from Postgres
    # ------------------------------------------------------------------
    results_df = pd.read_sql(
        "SELECT * FROM analytics_intermediate.int_castle__productivity_02_results",
        engine
    )
    employee_df = pd.read_sql(
        "SELECT * FROM analytics_intermediate.int_castle__productivity_03_employee_payouts",
        engine
    )
    earned_df = pd.read_sql(
        "SELECT * FROM analytics_intermediate.int_castle__productivity_01_earnedhrs_detail",
        engine
    )
    targets_df = pd.read_sql(
        "SELECT * FROM analytics_reference.ref_piptargets",
        engine
    )
    timings_df = pd.read_sql(
        "SELECT * FROM analytics_reference.ref_piptargettimings",
        engine
    )

    engine.dispose()

    # ------------------------------------------------------------------
    # 2. Resolve which target year applies for this output month
    # ------------------------------------------------------------------
    timing_row = timings_df[
        (timings_df["year"] == output_year) & (timings_df["month"] == output_month)
    ]
    target_year = int(timing_row["target_year_used"].iloc[0]) if not timing_row.empty else output_year

    # ------------------------------------------------------------------
    # 3. Filter to output period
    # ------------------------------------------------------------------
    month_results_df = results_df[
        (results_df["year"] == output_year) & (results_df["month"] == output_month)
    ]
    month_employee_df = employee_df[
        (employee_df["year"] == output_year) & (employee_df["month"] == output_month)
    ]
    month_earned_df = earned_df[
        (earned_df["year"] == output_year) &
        (earned_df["month"] == output_month) &
        (earned_df["include_flag"] == True)
    ].sort_values("date_completed")

    # ------------------------------------------------------------------
    # 4. Master summary file (all branches)
    # ------------------------------------------------------------------
    save_dir = os.path.join("reports", "productivity", "results", str(output_year), f"{output_month:02d}")
    os.makedirs(save_dir, exist_ok=True)

    master_path = os.path.join(save_dir, f"productivity_incentive_{output_year}_{output_month:02d}.xlsx")

    with pd.ExcelWriter(master_path, engine="xlsxwriter") as writer:
        workbook = writer.book
        fmt_pct      = workbook.add_format({"num_format": "0.00%"})
        fmt_currency = workbook.add_format({"num_format": "$#,##0.00"})
        fmt_number   = workbook.add_format({"num_format": "#,##0"})

        def apply_col_formats(df, sheet_name):
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            for i, col in enumerate(df.columns):
                if col.endswith("_pct"):
                    ws.set_column(i, i, 14, fmt_pct)
                elif col.endswith("_usd") or col.endswith("_bonus"):
                    ws.set_column(i, i, 14, fmt_currency)
                elif col.endswith("_hrs"):
                    ws.set_column(i, i, 12, fmt_number)

        apply_col_formats(month_results_df.sort_values(["country", "org"]).drop(columns=["uom"], errors="ignore"), "branch_results")
        apply_col_formats(month_employee_df.sort_values(["country", "org"]).drop(columns=["uom"], errors="ignore"), "employee_payouts")

    print(f"Master file written: {master_path}")

    # ------------------------------------------------------------------
    # 5. Per-branch files
    # ------------------------------------------------------------------
    for org in [o for o in BRANCHES if o in orgs]:

        file_path = os.path.join(save_dir, f"productivity_incentive_{output_year}_{output_month:02d}_{org}.xlsx")

        # Results tab — all months that share the same target_year_used
        target_year_months = timings_df[timings_df["target_year_used"] == target_year][["year", "month"]]
        org_results = results_df.merge(target_year_months, on=["year", "month"])
        org_results = org_results[org_results["org"] == org][[
            "org", "year", "month",
            "earned_hrs", "worked_reg_hrs", "worked_ot_hrs", "worked_total_hrs",
            "productivity_pct", "band_pct", "payout_usd"
        ]].sort_values(["year", "month"])

        # Targets table (placed at col L on results sheet)
        org_targets = targets_df[
            (targets_df["org"] == org) &
            (targets_df["year"] == target_year) &
            (targets_df["band_pct"] != 0)
        ][["org", "band_pct", "goal_pct", "payout_usd"]].sort_values("band_pct", ascending=False)

        # Worked tab
        org_worked = month_employee_df[month_employee_df["org"] == org][[
            "country", "org", "year", "month",
            "employee_id", "employee_name", "dept_code",
            "regular_hrs", "overtime_hrs", "total_hrs",
            "payout_usd", "month_bonus"
        ]]

        # Earned tab — include_flag already filtered above
        org_earned = month_earned_df[month_earned_df["org"] == org][[
            "org", "year", "month", "date_completed",
            "dj_nbr", "operation_code", "dj_quantity_completed",
            "product_form", "product_commodity", "product_grade", "product_item_number",
            "comp_complete_lbs", "job_status",
            "earned_hrs"
        ]]

        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            workbook = writer.book

            # --- Formats ---
            fmt_center   = workbook.add_format({"align": "center"})
            fmt_number   = workbook.add_format({"num_format": "#,##0"})
            fmt_number2  = workbook.add_format({"num_format": "#,##0.00"})
            fmt_pct2     = workbook.add_format({"num_format": "0.00%"})
            fmt_pct0     = workbook.add_format({"num_format": "0%"})
            fmt_currency = workbook.add_format({"num_format": "$#,##0.00"})

            # --- Results sheet ---
            org_results.to_excel(writer, sheet_name="results", index=False, startrow=0, startcol=0)
            org_targets.to_excel(writer, sheet_name="results", index=False, startrow=0, startcol=11)

            ws = writer.sheets["results"]
            ws.set_column(0, 0, 6,  fmt_center)   # org
            ws.set_column(1, 1, 6,  fmt_center)   # year
            ws.set_column(2, 2, 6,  fmt_center)   # month
            ws.set_column(3, 3, 12, fmt_number)   # earned_hrs
            ws.set_column(4, 4, 12, fmt_number)   # worked_reg_hrs
            ws.set_column(5, 5, 12, fmt_number)   # worked_ot_hrs
            ws.set_column(6, 6, 12, fmt_number)   # worked_total_hrs
            ws.set_column(7, 7, 14, fmt_pct2)     # productivity_pct
            ws.set_column(8, 8, 10, fmt_pct0)     # band_pct
            ws.set_column(9, 9, 12, fmt_currency) # payout_usd
            ws.set_column(11, 11, 6,  fmt_center)  # targets: org
            ws.set_column(12, 12, 10, fmt_pct0)    # targets: band_pct
            ws.set_column(13, 13, 10, fmt_pct2)    # targets: goal_pct
            ws.set_column(14, 14, 12, fmt_currency) # targets: payout_usd

            # --- Worked sheet ---
            org_worked.to_excel(writer, sheet_name="worked", index=False, startrow=0, startcol=0)
            ws = writer.sheets["worked"]
            ws.set_column(0, 0, 8,  fmt_center)   # country
            ws.set_column(1, 1, 6,  fmt_center)   # org
            ws.set_column(2, 2, 6,  fmt_center)   # year
            ws.set_column(3, 3, 6,  fmt_center)   # month
            ws.set_column(10, 10, 12, fmt_currency) # payout_usd
            ws.set_column(11, 11, 12, fmt_currency) # month_bonus

            # --- Earned sheet ---
            org_earned.to_excel(writer, sheet_name="earned", index=False, startrow=0, startcol=0)
            ws = writer.sheets["earned"]
            ws.set_column(0, 0, 6,  fmt_center)   # org
            ws.set_column(1, 1, 6,  fmt_center)   # year
            ws.set_column(2, 2, 6,  fmt_center)   # month
            ws.set_column(3, 3, 12, None)          # date_completed
            ws.set_column(7, 7, 12, fmt_number2)  # dj_quantity_completed
            ws.set_column(13, 13, 12, fmt_number2) # comp_complete_lbs
            ws.set_column(14, 14, 12, fmt_number2) # earned_hrs

        print(f"  {org} written: {file_path}")

    print(f"\nAll files written to {save_dir}")
