from etl.extract.sharepoint.sharepoint_extract import extract_open_orders
from etl.load.postgres_write import write_postgres_table


def ingest_cx_orders():
    df = extract_open_orders()
    write_postgres_table(df, table="cx_orders", schema="raw", if_exists="truncate")
