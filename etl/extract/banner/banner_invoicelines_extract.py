import pandas as pd
from etl.utils.connect_lakehouse import get_lakehouse_connection


def extract_banner_invoicelines():
    query = """
        SELECT
            dataAreaId,
            InvoiceId,
            InvoiceDate,
            LineNum,
            InventQty,
            DlvDate,
            Qty,
            SalesUnit,
            SalesId,
            SalesPrice,
            ItemId,
            LineAmount
        FROM dbo.CustInvoiceTransBiEntitiesV2;
    """

    with get_lakehouse_connection() as conn:
        df = pd.read_sql(query, conn)

    return df




