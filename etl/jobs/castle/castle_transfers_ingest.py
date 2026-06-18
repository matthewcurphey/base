from etl.extract.castle.castle_transfers_extract import extract_castle_transfers
from etl.load.postgres_write import write_postgres_table


def ingest_castle_transfers():
    df = extract_castle_transfers()
    write_postgres_table(df, table="castle_transfers", schema="raw", if_exists="truncate")
