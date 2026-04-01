import os
import pandas as pd

from config.paths import HR_RAW_DIR
from etl.utils.read_csv import read_csv

STANDARD_COLS = ["country", "org", "year", "month", "employee_id", "employee_name", "dept_code",
                 "regular_hrs", "overtime_hrs", "total_hrs"]


def extract_hr_mexico(year: int, branch_info_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract and transform Mexico payroll hours for a given year.

    Reads the cumulative annual file mx_worked_hours_YYYY.csv.
    Filters to employees on the PIP (Plant - Processing) incentive plan.

    Returns a DataFrame with columns: country, org, year, month, employee_id,
    employee_name, dept_code, regular_hrs, overtime_hrs, total_hrs.
    """

    filepath = os.path.join(HR_RAW_DIR, f"mx_worked_hours_{year}.csv")
    df = read_csv(filepath, dtype=str, encoding="ISO-8859-1")

    # Parse date and extract year/month
    df["Date"] = pd.to_datetime(df["Month"])
    df["month"] = df["Date"].dt.month
    df["year"] = df["Date"].dt.year

    # Map location → org via branch_info
    df = pd.merge(df, branch_info_df, how="left", left_on="Location", right_on="org_name").reset_index(drop=True)

    # Build a unique employee identifier combining ID and incentive plan
    df["employee_id"] = (
        df["Employee ID."].fillna(0).astype(float).astype(int).astype(str)
        + " - "
        + df["Incentive Plan"]
    )

    # Calculate hours
    df["Over Time Hours"] = pd.to_numeric(df["Over Time Hours"], errors="coerce").fillna(0)
    df["Total hours worked for this period"] = pd.to_numeric(
        df["Total hours worked for this period"], errors="coerce"
    ).fillna(0)
    df["regular_hrs"] = df["Total hours worked for this period"] - df["Over Time Hours"]

    # Filter logic differs by year — column name changed in 2026
    if year < 2026:
        df = df[df["Department"] == "Plant - Processing"].reset_index(drop=True)
    else:
        df = df[df["Incentive Plan"] == "PIP"].reset_index(drop=True)

    df = df.rename(columns={
        "org_country": "country",
        "org_code": "org",
        "Name": "employee_name",
        "Dept #": "dept_code",
        "Over Time Hours": "overtime_hrs",
        "Total hours worked for this period": "total_hrs",
    })

    df = df.astype({
        "year": "int64",
        "month": "int64",
        "dept_code": "object",
    })

    return df[STANDARD_COLS]
