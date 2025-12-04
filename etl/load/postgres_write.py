import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from etl.utils.connect_postgres import get_postgres_connection


def write_postgres_table(df, table, schema, if_exists="replace"):
    """
    Save a DataFrame to Postgres.
    If table does not exist and if_exists="truncate", it will be created automatically.
    """

    conn = get_postgres_connection()
    cur = conn.cursor()

    full_table = f"{schema}.{table}"

    # ---------------------------------
    # Handle replace mode (drop + create)
    # ---------------------------------
    if if_exists == "replace":
        cur.execute(f'DROP TABLE IF EXISTS {full_table} CASCADE;')
        columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
        cur.execute(f'CREATE TABLE {full_table} ({columns_sql});')

    # ---------------------------------
    # Handle truncate mode
    # ---------------------------------
    elif if_exists == "truncate":
        try:
            cur.execute(f"TRUNCATE TABLE {full_table};")
        except psycopg2.errors.UndefinedTable:
            # Table does NOT exist — create it automatically
            conn.rollback()  # Reset failed transaction
            columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
            cur.execute(f'CREATE TABLE {full_table} ({columns_sql});')

    # ---------------------------------
    # Insert data
    # ---------------------------------
    rows = [
        tuple(None if pd.isna(x) else str(x) for x in row)
        for row in df.to_numpy()
    ]

    cols = ", ".join([f'"{col}"' for col in df.columns])
    insert_sql = f"INSERT INTO {full_table} ({cols}) VALUES %s"

    execute_values(cur, insert_sql, rows)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(df)} rows into {full_table}.")

