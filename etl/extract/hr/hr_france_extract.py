import os
import pandas as pd

from config.paths import HR_RAW_DIR
from etl.utils.read_csv import read_csv

STANDARD_COLS = ["country", "org", "year", "month", "employee_id", "employee_name", "dept_code",
                 "regular_hrs", "overtime_hrs", "total_hrs"]

# Maps France branch codes to org codes
BRANCH_TO_ORG = {451: "ENA", 453: "ENT"}

# Only include plant processing departments
INCLUDED_DEPTS = ["120", "121"]


def extract_hr_france(year: int) -> pd.DataFrame:
    """
    Extract and transform France payroll hours for a given year.

    Reads the cumulative annual file fr_worked_hours_YYYY.csv.
    Filters to plant processing departments (120, 121).

    Returns a DataFrame with columns: country, org, year, month, employee_id,
    employee_name, dept_code, regular_hrs, overtime_hrs, total_hrs.
    """

    filepath = os.path.join(HR_RAW_DIR, f"fr_worked_hours_{year}.csv")
    df = read_csv(filepath, dtype=str, encoding="ISO-8859-1")

    # Map branch code (int) to org code
    df["Branch"] = pd.to_numeric(df["Branch"], errors="coerce")
    df["org"] = df["Branch"].map(BRANCH_TO_ORG)

    df["country"] = "FR"

    df["Overtime hrs"] = pd.to_numeric(df["Overtime hrs"], errors="coerce").fillna(0)
    df["Working hours hrs"] = pd.to_numeric(df["Working hours hrs"], errors="coerce").fillna(0)
    df["regular_hrs"] = df["Working hours hrs"] - df["Overtime hrs"]

    df = df.rename(columns={
        "Year": "year",
        "Month": "month",
        "Castle connect": "employee_id",
        "Name": "employee_name",
        "Dpt": "dept_code",
        "Overtime hrs": "overtime_hrs",
        "Working hours hrs": "total_hrs",
    })

    df = df.astype({
        "employee_id": "object",
        "dept_code": "object",
        "year": "int64",
        "month": "int64",
    })

    # Filter to plant processing departments — compare as strings
    df = df[df["dept_code"].isin(INCLUDED_DEPTS)].reset_index(drop=True)

    return df[STANDARD_COLS]
