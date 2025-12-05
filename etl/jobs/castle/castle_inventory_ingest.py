from etl.extract.castle.castle_inventory_extract import extract_castle_inventory
from etl.load.postgres_write import write_postgres_table

def ingest_castle_inventory():
    df = extract_castle_inventory()
    write_postgres_table(df, table="castle_inventory", schema="raw", if_exists="truncate")
