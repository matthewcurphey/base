from etl.extract.banner.banner_customers_extract import extract_banner_customers
from etl.load.postgres_write import write_postgres_table

def ingest_banner_customers():
    df = extract_banner_customers()
    write_postgres_table(df, table="banner_customers", schema="raw", if_exists="truncate")
