import pandas as pd
from etl.utils.connect_lakehouse import get_lakehouse_connection


def extract_banner_salesorderlines():
    """
    Extract Banner sales order line data from the Fabric Lakehouse SQL endpoint.
    Returns a Pandas DataFrame exactly as the source provides it (no renaming).
    """

    query = """
        SELECT
            dataAreaId,
            InventoryLotId,
            SalesUnitSymbol,
            ShippingSiteId,
            LineNumber,
            LineDescription,
            ItemNumber,
            ShippingWarehouseId,
            RequestedReceiptDate,
            OrderedSalesQuantity,
            LineAmount,
            ProductSizeId,
            SalesPrice,
            SalesOrderNumber,
            SalesOrderLineStatus,
            DeliveryAddressDescription,
            RequestedShippingDate,
            DeliveryModeCode,
            CustomersLineNumber,
            CurrencyCode
        FROM dbo.SalesOrderLinesV3;
    """

    with get_lakehouse_connection() as conn:
        df = pd.read_sql(query, conn)

    return df



