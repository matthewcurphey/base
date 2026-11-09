from etl.extract.oracle.oracle_transfer_transactions_extract import extract_castle_oracle_transfer_shipments
from etl.load.postgres_write import write_postgres_table


def ingest_castle_oracle_transfer_shipments():
    df = extract_castle_oracle_transfer_shipments()
    write_postgres_table(df, table="castle_oracle_transfer_shipments", schema="raw", if_exists="truncate")
