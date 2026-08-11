import subprocess
import sys

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

    # ActiveDirectoryInteractive spawns a sign-in popup and blocks this call
    # until it's dismissed. The watcher has to be a separate OS process, not
    # a thread in this same process — empirically, driving the popup from a
    # thread here fails silently every time, while an independent process
    # targeting the same popup works reliably (see lakehouse_auth.py).
    subprocess.Popen(
        [sys.executable, "-m", "etl.utils.lakehouse_auth", LAKEHOUSE_CONFIG["login_email"]],
    )

    return pyodbc.connect(conn_str)

