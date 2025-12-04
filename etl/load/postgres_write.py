import pandas as pd
from psycopg2.extras import execute_values
from etl.utils.connect_postgres import get_postgres_connection

def write_postgres_table(df, table, schema, if_exists="replace"):
    """
    Save a DataFrame to Postgres using ELT conventions.
    Modes:
      - replace: drop + recreate table (schema-changing)
      - truncate: keep table structure, just empty + load
      - append: add new rows only
    """
    conn = get_postgres_connection()
    cur = conn.cursor()

    full_table = f"{schema}.{table}"
    cols = ", ".join([f'"{col}"' for col in df.columns])
    rows = [
        tuple(None if pd.isna(x) else str(x) for x in row)
        for row in df.to_numpy()
    ]

    # 1) REPLACE MODE — destructive
    if if_exists == "replace":
        print(f"[REPLACE] Dropping and recreating {full_table}...")
        cur.execute(f"DROP TABLE IF EXISTS {full_table} CASCADE;")
        col_defs = ", ".join([f'"{col}" TEXT' for col in df.columns])
        cur.execute(f"CREATE TABLE {full_table} ({col_defs});")

    # 2) TRUNCATE MODE — SAFE DAILY INGESTION
    elif if_exists == "truncate":
        print(f"[TRUNCATE] Keeping table structure for {full_table}...")
        cur.execute(f"TRUNCATE TABLE {full_table};")

    # 3) APPEND MODE — simple inserts
    elif if_exists == "append":
        print(f"[APPEND] Inserting new rows into {full_table}...")

    else:
        raise ValueError("if_exists must be 'replace', 'truncate', or 'append'")

    # Insert data
    insert_sql = f"INSERT INTO {full_table} ({cols}) VALUES %s"
    execute_values(cur, insert_sql, rows)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(df)} rows into {full_table}.")
