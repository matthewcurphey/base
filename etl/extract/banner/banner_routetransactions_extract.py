import pandas as pd
from etl.utils.connect_lakehouse import get_lakehouse_connection


def extract_banner_routetransactions():
    """
    Extract Banner route transactions data from the Fabric Lakehouse SQL endpoint.
    Returns a Pandas DataFrame exactly as the source provides it (no renaming).
    """

    query = """
        SELECT
            dataAreaId,
            ProductionOrderNumber,
            OperationNumber,
            ProcessQuantity,
            EstimatedProcessTime,
            ScheduledFromDate,
            ScheduledEndDate,
            EstimatedOperationQuantity,
            OperationId,
            RouteOperationSequence,
            ProcessTime

        FROM dbo.ProductionOrderRouteOperations;
    """

    with get_lakehouse_connection() as conn:
        df = pd.read_sql(query, conn)

    return df



