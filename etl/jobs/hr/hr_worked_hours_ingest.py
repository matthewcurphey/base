import pandas as pd

from etl.extract.hr.hr_us_can_extract import extract_hr_us_can
from etl.extract.hr.hr_france_extract import extract_hr_france
from etl.extract.hr.hr_mexico_extract import extract_hr_mexico
from etl.extract.hr.hr_singapore_extract import extract_hr_singapore
from etl.extract.hr.hr_china_extract import extract_hr_china
from etl.load.postgres_write import write_postgres_table
from etl.utils.connect_postgres import get_postgres_connection

# Years to process. Extend this list each year or drive it from config.
INGEST_YEARS = [2025, 2026]


def _load_branch_info() -> pd.DataFrame:
    """
    Pull org reference data from postgres.
    Used by US/CAN and Mexico extracts to map site names → org codes and countries.
    Expects a table with at least InvOrgName, InvOrgCode, Country columns.
    """
    engine = get_postgres_connection()
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM analytics_reference.ref_orginfo", conn)
    engine.dispose()
    return df


def ingest_hr_worked_hours():
    """
    Orchestrate the full worked hours ingest across all countries and years.

    Loads branch reference data once, runs each country extract per year,
    concatenates all results, and writes to raw.hr_worked_hours (replace).
    """

    branch_info_df = _load_branch_info()

    all_dfs = []

    for year in INGEST_YEARS:
        print(f"  Extracting US/CAN {year}...")
        all_dfs.append(extract_hr_us_can(year, branch_info_df))

        print(f"  Extracting France {year}...")
        all_dfs.append(extract_hr_france(year))

        print(f"  Extracting Mexico {year}...")
        all_dfs.append(extract_hr_mexico(year, branch_info_df))

        print(f"  Extracting Singapore {year}...")
        all_dfs.append(extract_hr_singapore(year))

        print(f"  Extracting China {year}...")
        all_dfs.append(extract_hr_china(year))

    combined = pd.concat(all_dfs, ignore_index=True)
    combined = combined[combined["total_hrs"] != 0].reset_index(drop=True)

    write_postgres_table(combined, table="hr_workedhours", schema="raw", if_exists="replace")
