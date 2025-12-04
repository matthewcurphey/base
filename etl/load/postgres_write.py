# the schema defines which postgres schema it goes into

import pandas as pd
from psycopg2.extras import execute_values
from etl.utils.connect_postgres import get_postgres_connection

def write_postgres_table(df, table, schema, if_exists="replace"):
    """
    Save a DataFrame to the raw schema in Postgres.
    Stores all columns as TEXT. ELT standard.
    """
    conn = get_postgres_connection()
    cur = conn.cursor()

    full_table = f"{schema}.{table}"

    # Drop table if replacing
    if if_exists == "replace":
        cur.execute(f"DROP TABLE IF EXISTS {full_table} CASCADE;")
        columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
        cur.execute(f"CREATE TABLE {full_table} ({columns_sql});")

    # Convert df -> rows
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
