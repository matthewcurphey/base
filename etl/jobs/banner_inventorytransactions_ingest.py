from etl.extract.banner_inventorytransactions_extract import extract_banner_inventorytransactions
from etl.load.postgres_write import write_postgres_table

def ingest_banner_inventorytransactions():
    df = extract_banner_inventorytransactions()
    write_postgres_table(df, table="banner_inventorytransactions", schema="raw")
