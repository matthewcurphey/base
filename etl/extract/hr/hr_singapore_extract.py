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

# Regular hours are in columns 8–19 (Jan–Dec), OT in columns 21–32 (Jan.1–Dec.1)
REG_COLS_SLICE = slice(8, 20)
OT_COLS_SLICE = slice(21, 33)

OT_MONTH_MAP = {f"{k}.1": v for k, v in MONTH_MAP.items()}

# Filter to department 120 (Plant - Processing)
INCLUDED_DEPT = "120"


def extract_hr_singapore(year: int) -> pd.DataFrame:
    """
    Extract and transform Singapore payroll hours for a given year.

    Reads the cumulative annual file sg_worked_hours_YYYY.csv (2 header rows).
    Regular and overtime hours are in separate column blocks and are melted
    into long format before combining.

    Returns a DataFrame with columns: country, org, year, month, employee_id,
    employee_name, dept_code, regular_hrs, overtime_hrs, total_hrs.
    """

    filepath = os.path.join(HR_RAW_DIR, f"sg_worked_hours_{year}.csv")
    df = read_csv(filepath, dtype=str, encoding="ISO-8859-1", skiprows=2)

    # Keep only the numeric part of the department code
    df["Department"] = df["Department"].str.extract(r"(^\d+)")

    id_vars = ["Employee Number", "Employee Name", "Department"]

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
        })
        melted["hours"] = pd.to_numeric(melted["hours"], errors="coerce").fillna(0)
        melted[hours_col_name] = melted["hours"]
        return melted[["employee_id", "employee_name", "dept_code", "month", hours_col_name]]

    reg_df = _melt_hours(df, REG_COLS_SLICE, MONTH_MAP, "regular_hrs")
    ot_df = _melt_hours(df, OT_COLS_SLICE, OT_MONTH_MAP, "overtime_hrs")

    merged = pd.merge(reg_df, ot_df, on=["employee_id", "employee_name", "dept_code", "month"], how="outer").fillna(0)

    merged["country"] = "SG"
    merged["org"] = "SGP"
    merged["year"] = year
    merged["total_hrs"] = merged["regular_hrs"] + merged["overtime_hrs"]

    # Normalise employee_id
    merged["employee_id"] = (
        pd.to_numeric(merged["employee_id"], errors="coerce")
        .astype("Int64")
        .astype(str)
    )

    merged = merged[merged["dept_code"] == INCLUDED_DEPT].reset_index(drop=True)
    merged = merged[merged["total_hrs"] != 0].reset_index(drop=True)

    return merged[STANDARD_COLS]
