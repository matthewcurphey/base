"""
Master ingestion pipeline runner.
Executes all extract → raw load jobs in sequence.
"""

from etl.jobs.banner_customers_ingest import ingest_banner_customers
from etl.jobs.banner_inventorytransactions_ingest import ingest_banner_inventorytransactions


def run_all_ingestions():
    print("Starting ingestion pipeline...")

    # Customer master
    print("Ingesting Banner Customers...")
    ingest_banner_customers()
    print("Banner Customers done.\n")

    print("Ingesting Banner Inventory Transactions...")
    ingest_banner_inventorytransactions()
    print("Banner Inventory Transactions done.\n")

    # Add more as you build them:
    # print("→ Ingesting Banner Sales...")
    # ingest_banner_sales()
    # print("✓ Banner Sales done.\n")

    print("All ingestion jobs completed successfully!")

if __name__ == "__main__":
    run_all_ingestions()
