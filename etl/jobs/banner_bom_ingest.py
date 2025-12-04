from etl.extract.banner_bom_extract import extract_banner_bom
from etl.load.postgres_write import write_postgres_table

def ingest_banner_bom():
    df = extract_banner_bom()
    write_postgres_table(df, table="banner_bom", schema="raw")
