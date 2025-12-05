# import psycopg2
# from config.postgres import POSTGRES_CONFIG

# def get_postgres_connection():
#     return psycopg2.connect(
#         host=POSTGRES_CONFIG["host"],
#         port=POSTGRES_CONFIG["port"],
#         database=POSTGRES_CONFIG["database"],
#         user=POSTGRES_CONFIG["user"],
#         password=POSTGRES_CONFIG["password"]
#     )

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from config.postgres import POSTGRES_CONFIG

def get_postgres_connection():
    """
    Return a SQLAlchemy engine instead of a raw psycopg2 connection.
    This is a drop-in replacement for the old psycopg2-based function.
    """

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"],
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
        database=POSTGRES_CONFIG["database"],
    )

    engine = create_engine(url, future=True)
    return engine
