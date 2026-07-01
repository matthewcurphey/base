from etl.extract.oracle.oracle_inventory_extract import extract_castle_oracle_inventory
from etl.load.postgres_write import write_postgres_table


def ingest_castle_oracle_inventory():
    df = extract_castle_oracle_inventory()
    write_postgres_table(df, table="castle_oracle_inventory", schema="raw", if_exists="truncate")
