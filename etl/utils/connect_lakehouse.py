import pyodbc
from config.lakehouse import LAKEHOUSE_CONFIG


def get_lakehouse_connection():
    """
    Returns an open connection to the Fabric Lakehouse SQL Endpoint.
    Mirrors the style of get_postgres_connection().
    """

    conn_str = (
        f"Driver={{{LAKEHOUSE_CONFIG['driver']}}};"
        f"Server={LAKEHOUSE_CONFIG['server']};"
        f"Database={LAKEHOUSE_CONFIG['database']};"
        f"Authentication={LAKEHOUSE_CONFIG['authentication']};"
        f"Encrypt={LAKEHOUSE_CONFIG['encrypt']};"
        f"TrustServerCertificate={LAKEHOUSE_CONFIG['trust_cert']};"
    )

    return pyodbc.connect(conn_str)

