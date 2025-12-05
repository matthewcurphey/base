from etl.extract.castle.castle_sales_extract import extract_castle_sales
from etl.load.postgres_write import write_postgres_table

def ingest_castle_sales():
    df = extract_castle_sales()
    write_postgres_table(df, table="castle_sales", schema="raw", if_exists="truncate")
