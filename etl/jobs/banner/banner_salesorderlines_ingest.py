from etl.extract.banner.banner_salesorderlines_extract import extract_banner_salesorderlines
from etl.load.postgres_write import write_postgres_table

def ingest_banner_salesorderlines():
    df = extract_banner_salesorderlines()
    write_postgres_table(df, table="banner_salesorderlines", schema="raw", if_exists="truncate")
