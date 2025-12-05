from etl.extract.banner.banner_invoiceheaders_extract import extract_banner_invoiceheaders
from etl.load.postgres_write import write_postgres_table

def ingest_banner_invoiceheaders():
    df = extract_banner_invoiceheaders()
    write_postgres_table(df, table="banner_invoiceheaders", schema="raw", if_exists="truncate")
