import pandas as pd
from etl.utils.connect_lakehouse import get_lakehouse_connection


def extract_banner_bom():
    """
    Extract Banner bill of materials data from the Fabric Lakehouse SQL endpoint.
    Returns a Pandas DataFrame exactly as the source provides it (no renaming).
    """

    query = """
        SELECT 
            ProductionOrderNumber,
            ItemNumber,
            LineNumber,
            BOMLineQuantity,
            BOMLineQuantityDenominator,
            StartedInventoryQuantity,
            StartedBOMLineQuantity,
            RemainingBOMLineQuantity,
            RemainingInventoryQuantity,
            ReleasedBOMLineQuantity,
            SourceBOMId,
            EstimatedBOMLineQuantity,
            ProductSizeId,
            dataAreaId

        FROM dbo.ProductionOrderBillOfMaterialLines;
    """

    with get_lakehouse_connection() as conn:
        df = pd.read_sql(query, conn)

    return df



