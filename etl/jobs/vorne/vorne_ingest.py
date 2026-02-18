from etl.extract.vorne.vorne_extract import extract_vorne
from etl.load.postgres_write import write_postgres_table
from datetime import date

def ingest_vorne():
    df = extract_vorne()
    
    write_postgres_table(df, table="vorne", schema="raw", if_exists="truncate")
