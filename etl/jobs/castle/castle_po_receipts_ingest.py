from etl.extract.castle.castle_po_receipts_extract import extract_castle_po_receipts
from etl.load.postgres_write import write_postgres_table


def ingest_castle_po_receipts():
    df = extract_castle_po_receipts()
    write_postgres_table(df, table="castle_po_receipts", schema="raw", if_exists="truncate")
