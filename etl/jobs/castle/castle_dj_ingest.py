from etl.extract.castle.castle_dj_extract import extract_castle_dj
from etl.load.postgres_write import write_postgres_table

def ingest_castle_dj():
    df = extract_castle_dj()
    write_postgres_table(df, table="castle_dj", schema="raw", if_exists="truncate")
