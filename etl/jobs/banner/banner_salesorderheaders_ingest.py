from etl.extract.banner.banner_salesorderheaders_extract import extract_banner_salesorderheaders
from etl.load.postgres_write import write_postgres_table

def ingest_banner_salesorderheaders():
    df = extract_banner_salesorderheaders()
    write_postgres_table(df, table="banner_salesorderheaders", schema="raw", if_exists="truncate")
