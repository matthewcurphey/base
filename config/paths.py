import os

# Base directory of the project
# config/paths.py → config → base → C:\base
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# Path to the ETL folder
ETL_DIR = os.path.join(BASE_DIR, "etl")

# Path to raw CSV data
DATA_RAW_DIR = os.path.join(ETL_DIR, "data_raw")

# Domain-specific raw data folders
CASTLE_RAW_DIR = os.path.join(DATA_RAW_DIR, "castle")
BANNER_RAW_DIR = os.path.join(DATA_RAW_DIR, "banner")

