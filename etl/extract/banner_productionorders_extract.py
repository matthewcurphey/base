import pandas as pd
from etl.utils.connect_lakehouse import get_lakehouse_connection


def extract_banner_productionorders():
    """
    Extract Banner production order data from the Fabric Lakehouse SQL endpoint.
    Returns a Pandas DataFrame exactly as the source provides it (no renaming).
    """

    query = """
    SELECT
        -- Production Order Headers (P)
        P.dataAreaId,
        P.ProductionOrderNumber,
        P.ItemNumber,
        P.ProductionOrderStatus,
        P.StartedDate,
        P.DeliveryDate,
        P.ProductSizeId,
        P.DemandSalesOrderNumber,
        P.SourceBOMVersionValidityDate,
        P.EstimatedQuantity,
        P.EndedDate,
        P.ProductionSiteId,
        P.SourceBOMId,
        P.DemandSalesOrderLineInventoryLotId,
        P.DemandProductionOrderLineNumber,

        -- Sales Order Lines (S)
        S.LineAmount,
        S.LineNumber,
        S.OrderedSalesQuantity,
        S.SalesUnitSymbol

    FROM dbo.ProductionOrderHeaders AS P
    LEFT JOIN dbo.SalesOrderLines AS S
        ON P.dataAreaId = S.dataAreaId
    AND P.DemandSalesOrderLineInventoryLotId = S.InventoryLotId;

    """

    with get_lakehouse_connection() as conn:
        df = pd.read_sql(query, conn)

    return df



