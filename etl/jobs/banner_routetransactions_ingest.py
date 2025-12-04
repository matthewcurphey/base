from etl.extract.banner_routetransactions_extract import extract_banner_routetransactions
from etl.load.postgres_write import write_postgres_table

def ingest_banner_routetransactions():
    df = extract_banner_routetransactions()
    write_postgres_table(df, table="banner_routetransactions", schema="raw")
