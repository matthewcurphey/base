import os

from dotenv import load_dotenv

load_dotenv()

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "analytics",
    "user": "postgres",
    "password": os.environ["POSTGRES_PASSWORD"],
    }