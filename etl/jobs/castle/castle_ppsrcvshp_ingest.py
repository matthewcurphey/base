from etl.extract.castle.castle_ppsrcvshp_extract import extract_castle_ppsrcvshp
from etl.load.postgres_write import write_postgres_table

def ingest_castle_ppsrcvshp():
    df = extract_castle_ppsrcvshp()
    write_postgres_table(df, table="castle_ppsrcvshp", schema="raw", if_exists="truncate")
