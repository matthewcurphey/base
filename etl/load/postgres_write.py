# import pandas as pd
# import psycopg2
# from psycopg2.extras import execute_values
# from etl.utils.connect_postgres import get_postgres_connection


# def write_postgres_table(df, table, schema, if_exists="replace"):
#     """
#     Save a DataFrame to Postgres.
#     If table does not exist and if_exists="truncate", it will be created automatically.
#     """

#     conn = get_postgres_connection()
#     cur = conn.cursor()

#     full_table = f"{schema}.{table}"

#     # ---------------------------------
#     # Handle replace mode (drop + create)
#     # ---------------------------------
#     if if_exists == "replace":
#         cur.execute(f'DROP TABLE IF EXISTS {full_table} CASCADE;')
#         columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
#         cur.execute(f'CREATE TABLE {full_table} ({columns_sql});')

#     # ---------------------------------
#     # Handle truncate mode
#     # ---------------------------------
#     elif if_exists == "truncate":
#         try:
#             cur.execute(f"TRUNCATE TABLE {full_table};")
#         except psycopg2.errors.UndefinedTable:
#             # Table does NOT exist — create it automatically
#             conn.rollback()  # Reset failed transaction
#             columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
#             cur.execute(f'CREATE TABLE {full_table} ({columns_sql});')

#     # ---------------------------------
#     # Insert data
#     # ---------------------------------
#     rows = [
#         tuple(None if pd.isna(x) else str(x) for x in row)
#         for row in df.to_numpy()
#     ]

#     cols = ", ".join([f'"{col}"' for col in df.columns])
#     insert_sql = f"INSERT INTO {full_table} ({cols}) VALUES %s"

#     execute_values(cur, insert_sql, rows)

#     conn.commit()
#     cur.close()
#     conn.close()

#     print(f"Loaded {len(df)} rows into {full_table}.")

import pandas as pd
import sqlalchemy
from etl.utils.connect_postgres import get_postgres_connection


def write_postgres_table(df, table, schema, if_exists="replace", dtype=None):
    """
    Write a DataFrame to Postgres using SQLAlchemy.
    Supports replace, truncate, append.
    """

    engine = get_postgres_connection()
    full_table = f'"{schema}"."{table}"'

    # --- Handle truncate manually (SQLAlchemy doesn't have truncate mode)
    if if_exists == "truncate":
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(f'TRUNCATE TABLE {full_table};'))
        if_exists = "append"  # write fresh data after truncate

    # --- Use pandas built-in SQL writer (safe, dtype-stable)
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=if_exists,  # now replace, append
        index=False,
        dtype=dtype # optional explicit type mapping
    )

    engine.dispose()
    print(f"Loaded {len(df)} rows into {schema}.{table}.")
