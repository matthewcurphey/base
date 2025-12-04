import pandas as pd
from etl.utils.connect_lakehouse import get_lakehouse_connection


def extract_banner_inventorytransactions():
    """
    Extract Banner inventory transactions data from the Fabric Lakehouse SQL endpoint.
    Returns a Pandas DataFrame exactly as the source provides it (no renaming).
    """

    query = """
        SELECT
        -- Transaction info
        T.VoucherPhysical,
        T.StatusReceipt,
        T.StatusIssue,
        T.ReturnInventTransOrigin,
        T.Qty,
        T.PackingSlipId,
        T.OriginReferenceId,
        T.OriginReferenceCategory,
        T.MarkingRefInventTransOrigin,
        T.ItemId,
        T.InvoiceId,
        T.InventTransRecId,
        T.InventTransOriginInventTransId,
        T.InventTransOriginIItemInventDimId,
        T.InventTransOrigin,
        T.InventSiteId,
        T.InventLocationId,
        T.inventDimId,
        T.FinancialVoucher,
        T.DatePhysical,
        T.DateFinancial,
        T.DateClosed,
        T.dataAreaId,
        T.CostAmountPosted,
        T.CostAmountPhysical,
        T.CostAmountOperations,
        T.CostAmountAdjustment,

        -- Dim info
        D.Size,
        D.BatchNumber

    FROM dbo.InventTrans AS T
    LEFT JOIN
        dbo.InventDim AS D
        ON T.dataAreaId = D.dataAreaId
    AND T.[inventDimId] = D.DimensionNumber;
    """

    with get_lakehouse_connection() as conn:
        df = pd.read_sql(query, conn)

    return df



