"""
Master ingestion pipeline runner.
Executes all extract → raw load jobs in sequence.
"""
# BANNER
from etl.jobs.banner.banner_customers_ingest import ingest_banner_customers
from etl.jobs.banner.banner_inventorytransactions_ingest import ingest_banner_inventorytransactions
from etl.jobs.banner.banner_bom_ingest import ingest_banner_bom
from etl.jobs.banner.banner_routetransactions_ingest import ingest_banner_routetransactions
from etl.jobs.banner.banner_productionorders_ingest import ingest_banner_productionorders
from etl.jobs.banner.banner_salesorderlines_ingest import ingest_banner_salesorderlines
from etl.jobs.banner.banner_salesorderheaders_ingest import ingest_banner_salesorderheaders
from etl.jobs.banner.banner_invoicelines_ingest import ingest_banner_invoicelines
from etl.jobs.banner.banner_invoiceheaders_ingest import ingest_banner_invoiceheaders




def run_all_banner_ingestions():
    print("Starting Banner ingestion pipeline...")


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

    print("Ingesting Banner Sales Order Headers...")
    ingest_banner_salesorderheaders()
    print("Banner Sales Order Headers done.\n")

    print("Ingesting Banner Invoice Lines...")
    ingest_banner_invoicelines()
    print("Banner Invoice Lines done.\n")

    print("Ingesting Banner Invoice Headers...")
    ingest_banner_invoiceheaders()
    print("Banner Invoice Headers done.\n")


    print("All Banner ingestion jobs completed successfully!")


from etl.jobs.castle.castle_sales_ingest import ingest_castle_sales
from etl.jobs.castle.castle_dj_ingest import ingest_castle_dj
from etl.jobs.castle.castle_ppsrcvshp_ingest import ingest_castle_ppsrcvshp
from etl.jobs.castle.castle_inventory_ingest import ingest_castle_inventory

def run_all_castle_ingestions():
    print("Starting Castle ingestion pipeline...")


    print("Ingesting Castle Sales...")
    ingest_castle_sales()
    print("Castle Sales done.\n")

    print("Ingesting Castle DJ...")
    ingest_castle_dj()
    print("Castle DJ done.\n")

    print("Ingesting Castle PPS RCV SHP...")
    ingest_castle_ppsrcvshp()
    print("Castle PPS RCV SHP done.\n")

    print("Ingesting Castle Inventory...")
    ingest_castle_inventory()
    print("Castle Inventory done.\n")

    print("All Castle ingestion jobs completed successfully!")


from etl.jobs.vorne.vorne_ingest import ingest_vorne
def run_all_vorne_ingestions():
    print("Starting Vorne ingestion pipelines...")

    print("Imgesting Vorne Data...")
    ingest_vorne()
    print("Vorne Data done.")


from etl.jobs.fx.fxrates_ingest import ingest_fxrates
def run_all_other_ingestions():
    print("Starting Other ingestion pipelines...")

    print("Imgesting FX Rates...")
    ingest_fxrates()
    print("FX Rates done.")




def run_all_ingestions():
    run_all_banner_ingestions()
    run_all_castle_ingestions()

    run_all_vorne_ingestions()

    run_all_other_ingestions()
    

if __name__ == "__main__":
    run_all_ingestions()
