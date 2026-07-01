from etl.extract.oracle.oracle_di_extract import extract_castle_oracle_di
from etl.load.postgres_write import write_postgres_table


def ingest_castle_oracle_di():
    df = extract_castle_oracle_di()
    write_postgres_table(df, table="castle_oracle_di", schema="raw", if_exists="truncate")
