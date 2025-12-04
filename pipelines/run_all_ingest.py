"""
Master ingestion pipeline runner.
Executes all extract → raw load jobs in sequence.
"""

from etl.jobs.banner_customers_ingest import ingest_banner_customers
from etl.jobs.banner_inventorytransactions_ingest import ingest_banner_inventorytransactions
from etl.jobs.banner_bom_ingest import ingest_banner_bom
from etl.jobs.banner_routetransactions_ingest import ingest_banner_routetransactions
from etl.jobs.banner_productionorders_ingest import ingest_banner_productionorders
from etl.jobs.banner_salesorderlines_ingest import ingest_banner_salesorderlines
from etl.jobs.banner_invoicelines_ingest import ingest_banner_invoicelines
from etl.jobs.banner_invoiceheaders_ingest import ingest_banner_invoiceheaders


def run_all_ingestions():
    print("Starting ingestion pipeline...")

    # Customer master
    print("Ingesting Banner Customers...")
    ingest_banner_customers()
    print("Banner Customers done.\n")

    print("Ingesting Banner Inventory Transactions...")
    ingest_banner_inventorytransactions()
    print("Banner Inventory Transactions done.\n")

    print("Ingesting Banner BOMs...")
    ingest_banner_bom()
    print("Banner BOMs done.\n")

    print("Ingesting Banner Route Transactions...")
    ingest_banner_routetransactions()
    print("Banner Route Transactions done.\n")

    print("Ingesting Banner Production Orders...")
    ingest_banner_productionorders()
    print("Banner Production Orders done.\n")

    print("Ingesting Banner Sales Order Lines...")
    ingest_banner_salesorderlines()
    print("Banner Sales Order Lines done.\n")

    print("Ingesting Banner Invoice Lines...")
    ingest_banner_invoicelines()
    print("Banner Invoice Lines done.\n")

    print("Ingesting Banner Invoice Headers...")
    ingest_banner_invoiceheaders()
    print("Banner Invoice Headers done.\n")


    

    print("All ingestion jobs completed successfully!")

if __name__ == "__main__":
    run_all_ingestions()
