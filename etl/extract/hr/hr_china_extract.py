import os
import pandas as pd

from config.paths import HR_RAW_DIR
from etl.utils.read_csv import read_csv

STANDARD_COLS = ["country", "org", "year", "month", "employee_id", "employee_name", "dept_code",
                 "regular_hrs", "overtime_hrs", "total_hrs"]

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

OT_MONTH_MAP = {f"{k}.1": v for k, v in MONTH_MAP.items()}

# Regular hours in columns 6–17, OT in columns 19–30
REG_COLS_SLICE = slice(6, 18)
OT_COLS_SLICE = slice(19, 31)

LOCATION_TO_ORG = {"Shanghai": "ASC", "Chengdu": "ADC"}

PLANT_PROCESSING_DEPT = "Plant - Processing"


def extract_hr_china(year: int) -> pd.DataFrame:
    """
    Extract and transform China payroll hours for a given year.

    Reads the cumulative annual file cn_worked_hours_YYYY.csv (2 header rows).
    Filters to Plant - Processing department. Regular and overtime hours are in
    separate column blocks and are melted into long format before combining.

    Returns a DataFrame with columns: country, org, year, month, employee_id,
    employee_name, dept_code, regular_hrs, overtime_hrs, total_hrs.
    """

    filepath = os.path.join(HR_RAW_DIR, f"cn_worked_hours_{year}.csv")
    df = read_csv(filepath, dtype=str, encoding="ISO-8859-1", skiprows=2)

    # Filter to plant processing and normalise dept code
    df = df[df["Department"] == PLANT_PROCESSING_DEPT].reset_index(drop=True)
    df["Department"] = "120"

    id_vars = ["Employee Number", "Employee Name", "Department", "Location"]

    def _melt_hours(source_df, col_slice, month_map, hours_col_name):
        hour_cols = source_df.columns[col_slice]
        melted = source_df.melt(
            id_vars=id_vars,
            value_vars=hour_cols,
            var_name="month_label",
            value_name="hours",
        )
        melted["month"] = melted["month_label"].map(month_map)
        melted = melted.rename(columns={
            "Employee Number": "employee_id",
            "Employee Name": "employee_name",
            "Department": "dept_code",
            "Location": "org",
        })
        melted["hours"] = pd.to_numeric(melted["hours"], errors="coerce").fillna(0)
        melted[hours_col_name] = melted["hours"]
        return melted[["employee_id", "employee_name", "dept_code", "org", "month", hours_col_name]]

    reg_df = _melt_hours(df, REG_COLS_SLICE, MONTH_MAP, "regular_hrs")
    ot_df = _melt_hours(df, OT_COLS_SLICE, OT_MONTH_MAP, "overtime_hrs")

    merged = pd.merge(reg_df, ot_df, on=["employee_id", "employee_name", "dept_code", "org", "month"], how="outer").fillna(0)

    merged["country"] = "CN"
    merged["year"] = year
    merged["total_hrs"] = merged["regular_hrs"] + merged["overtime_hrs"]

    # Map location name → org code
    merged["org"] = merged["org"].map(LOCATION_TO_ORG)

    # Normalise employee_id
    merged["employee_id"] = (
        pd.to_numeric(merged["employee_id"], errors="coerce")
        .astype("Int64")
        .astype(str)
    )

    merged = merged[merged["total_hrs"] != 0].reset_index(drop=True)

    return merged[STANDARD_COLS]
