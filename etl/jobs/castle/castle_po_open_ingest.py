from etl.extract.castle.castle_po_open_extract import extract_castle_po_open
from etl.load.postgres_write import write_postgres_table


def ingest_castle_po_open():
    df = extract_castle_po_open()
    write_postgres_table(df, table="castle_po_open", schema="raw", if_exists="truncate")
