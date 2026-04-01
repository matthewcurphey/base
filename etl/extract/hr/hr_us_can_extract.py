import os
import glob
import pandas as pd

from config.paths import HR_RAW_DIR
from etl.utils.read_csv import read_csv

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

STANDARD_COLS = ["country", "org", "year", "month", "employee_id", "employee_name", "dept_code",
                 "regular_hrs", "overtime_hrs", "total_hrs"]


def extract_hr_us_can(year: int, branch_info_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract and transform US/Canada payroll hours for a given year.

    Reads all monthly CSV files matching us_can_worked_hours_MM-YYYY.csv,
    plus the annual temp hours file. Branch info (site → org/country mapping)
    is passed in from the job to avoid repeated DB reads.

    Returns a DataFrame with columns: country, org, year, month, employee_id,
    employee_name, dept_code, regular_hrs, overtime_hrs, total_hrs.
    """

    # ------------------------------------------------------------------
    # REGULAR HEADCOUNT — glob all monthly files for the year
    # ------------------------------------------------------------------
    pattern = os.path.join(HR_RAW_DIR, f"us_can_worked_hours_*-{year}.csv")
    monthly_files = sorted(glob.glob(pattern))

    if not monthly_files:
        raise FileNotFoundError(f"No US/CAN worked hours files found for {year} in {HR_RAW_DIR}")

    monthly_dfs = []
    for filepath in monthly_files:
        # Extract month number from filename: us_can_worked_hours_01-2026.csv → 1
        month_str = os.path.basename(filepath).split("_")[4].split("-")[0]
        month = int(month_str)

        df = read_csv(filepath, dtype=str, encoding="ISO-8859-1")
        df[["year", "month", "dept_code"]] = [year, month, "110/120"]
        monthly_dfs.append(df)

    concat_df = pd.concat(monthly_dfs, ignore_index=True)

    # Split Location field into components to get the site name
    concat_df[["group", "company", "site", "dept"]] = concat_df["Location"].str.split("/", expand=True)

    # Extract employee ID and name from combined field
    concat_df["employee_id"] = concat_df["Employee Name (ID)"].str.extract(r"\((\d+)\)")
    concat_df["employee_name"] = concat_df["Employee Name (ID)"].str.extract(r"^(.*) \(\d+\)")

    # Map site → org and country via branch_info
    merged_df = pd.merge(concat_df, branch_info_df, how="left", left_on="site", right_on="org_name")
    merged_df = merged_df.dropna(subset=["org_code"]).reset_index(drop=True)

    detail_df = (
        merged_df[["org_country", "org_code", "year", "month", "employee_id", "employee_name", "dept_code", "Pay Code", "Hours"]]
        .rename(columns={"org_country": "country", "org_code": "org", "Hours": "hours", "Pay Code": "pay_code"})
        .copy()
    )

    # Convert hours to numeric
    detail_df["hours"] = pd.to_numeric(detail_df["hours"], errors="coerce").fillna(0)

    # Pivot pay codes into columns
    detail_df = detail_df.pivot_table(
        index=["country", "org", "year", "month", "employee_id", "employee_name", "dept_code"],
        columns="pay_code",
        values="hours",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    detail_df = detail_df.rename(columns={
        "Regular": "regular_hrs",
        "Overtime": "overtime_hrs",
        "Doubletime": "doubletime_hrs",
    })

    # Ensure all expected columns exist (some pay codes may be absent in a given month)
    for col in ("regular_hrs", "overtime_hrs", "doubletime_hrs"):
        if col not in detail_df.columns:
            detail_df[col] = 0.0

    detail_df["overtime_hrs"] = detail_df["overtime_hrs"] + detail_df["doubletime_hrs"]
    detail_df["total_hrs"] = detail_df["regular_hrs"] + detail_df["overtime_hrs"]
    detail_df = detail_df.drop(columns=["doubletime_hrs"])

    us_can_df = (
        detail_df
        .groupby(["country", "org", "year", "month", "employee_id", "employee_name", "dept_code"])[
            ["regular_hrs", "overtime_hrs", "total_hrs"]
        ]
        .sum()
        .reset_index()
    )

    # ------------------------------------------------------------------
    # TEMP HOURS
    # ------------------------------------------------------------------
    temp_file = os.path.join(HR_RAW_DIR, f"us_can_temp_hours_{year}.csv")
    temp_df = read_csv(temp_file, dtype=str, encoding="ISO-8859-1")

    # Rows 1 and 2 (0-indexed) are regular and overtime; row 0 is a label row
    temp_df = temp_df.iloc[1:3].copy()
    temp_df.index = ["regular_hrs", "overtime_hrs"]

    temp_df = temp_df.set_index(temp_df.columns[0]).T.reset_index()
    temp_df.columns = ["month", "regular_hrs", "overtime_hrs"]

    temp_df["regular_hrs"] = pd.to_numeric(temp_df["regular_hrs"], errors="coerce").fillna(0)
    temp_df["overtime_hrs"] = pd.to_numeric(temp_df["overtime_hrs"], errors="coerce").fillna(0)

    # Extract week-of-month and month name from combined field e.g. "W1-Jan"
    temp_df[["week_of_month", "month"]] = temp_df["month"].str.split("-", expand=True)
    temp_df["month"] = temp_df["month"].map(MONTH_MAP)

    temp_df["country"] = "US"
    temp_df["org"] = "ATL"
    temp_df["year"] = year
    temp_df["employee_id"] = "999999"
    temp_df["employee_name"] = "TEMP"
    temp_df["dept_code"] = "110/120"
    temp_df["total_hrs"] = temp_df["regular_hrs"] + temp_df["overtime_hrs"]

    temp_df = (
        temp_df[STANDARD_COLS]
        .groupby(["country", "org", "year", "month", "employee_id", "employee_name", "dept_code"], as_index=False)[
            ["regular_hrs", "overtime_hrs", "total_hrs"]
        ]
        .sum()
    )

    # ------------------------------------------------------------------
    # COMBINE
    # ------------------------------------------------------------------
    combined = pd.concat([us_can_df, temp_df], ignore_index=True)
    combined = combined[combined["total_hrs"] != 0].reset_index(drop=True)

    return combined[STANDARD_COLS]
