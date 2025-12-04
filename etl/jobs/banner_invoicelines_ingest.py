from etl.extract.banner_invoicelines_extract import extract_banner_invoicelines
from etl.load.postgres_write import write_postgres_table

def ingest_banner_invoicelines():
    df = extract_banner_invoicelines()
    write_postgres_table(df, table="banner_invoicelines", schema="raw")
