import pandas as pd
from etl.utils.connect_lakehouse import get_lakehouse_connection


def extract_banner_salesorderheaders():
    """
    Extract Banner sales order line data from the Fabric Lakehouse SQL endpoint.
    Returns a Pandas DataFrame exactly as the source provides it (no renaming).
    """

    query = """
        SELECT
            dataAreaId,
            SalesOrderNumber,
            SalesOrderStatus,
            SalesOrderProcessingStatus,
            SalesOrderName,
            CustomerRequisitionNumber,
            CustomersOrderReference,
            InvoiceCustomerAccountNumber,
            OrderTotalAmount,
            OrderTotalChargesAmount,
            OrderTotalTaxAmount,
            CurrencyCode,

            -- Dates
            OrderCreationDateTime,
            RequestedReceiptDate,
            ConfirmedReceiptDate,
            RequestedShippingDate,
            ConfirmedShippingDate,

            -- Customer info
            Email,

            -- Delivery address
            DeliveryAddressName,
            DeliveryAddressStreet,
            DeliveryAddressCity,
            DeliveryAddressStateId,
            DeliveryAddressZipCode,
            DeliveryAddressCountryRegionId,

            -- Logistics
            DeliveryModeCode,
            DefaultShippingSiteId,
            DefaultShippingWarehouseId
        FROM dbo.SalesOrderHeaders;

    """


    with get_lakehouse_connection() as conn:
        df = pd.read_sql(query, conn)

    return df



