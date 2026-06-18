from etl.extract.fx.fxrates_extract import extract_fxrates
from etl.load.postgres_write import write_postgres_table
from datetime import date

def ingest_fxrates():
    df = extract_fxrates(
        start_date=date(2023, 1, 1),
        end_date=date.today()
    )
    write_postgres_table(df, table="fxrates", schema="raw", if_exists="truncate")
