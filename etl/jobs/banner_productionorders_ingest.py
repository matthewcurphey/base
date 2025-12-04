from etl.extract.banner_productionorders_extract import extract_banner_productionorders
from etl.load.postgres_write import write_postgres_table

def ingest_banner_productionorders():
    df = extract_banner_productionorders()
    write_postgres_table(df, table="banner_productionorders", schema="raw")
