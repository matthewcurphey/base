from etl.extract.oracle.oracle_open_orders_extract import extract_castle_oracle_open_orders
from etl.load.postgres_write import write_postgres_table


def ingest_castle_oracle_open_orders():
    df = extract_castle_oracle_open_orders()
    write_postgres_table(df, table="castle_oracle_open_orders", schema="raw", if_exists="truncate")
