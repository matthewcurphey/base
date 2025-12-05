import pandas as pd
from etl.utils.connect_lakehouse import get_lakehouse_connection


def extract_banner_invoiceheaders():
    """
    Extract Banner invoice lines data from the Fabric Lakehouse SQL endpoint.
    Returns a Pandas DataFrame exactly as the source provides it (no renaming).
    """

    query = """
        SELECT
            dataAreaId,
            InvoiceAccount,
            InvoiceAmount,
            OrderAccount,
            inventLocationId,
            InvoiceDate,
            CurrencyCode,
            Qty,
            InvoiceId,
            SalesId,
            PrintMgmtSiteId,
            DeliveryName,
            LedgerVoucher
        FROM dbo.CustInvoiceJourBiEntities;

    """

    with get_lakehouse_connection() as conn:
        df = pd.read_sql(query, conn)

    return df



