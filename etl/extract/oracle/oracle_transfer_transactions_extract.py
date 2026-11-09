import glob
import os

import pandas as pd

from config.paths import ORACLE_RAW_DIR

# Manual exports from Oracle View Material Transactions, one file per org.
# Oracle's .xls here is tab-delimited text. The export repeats columns once per
# form tab (Item, Transaction Date, Source, ...); pandas suffixes the repeats
# with .1/.2 and only the first occurrence is kept.
COLUMN_RENAME = {
    'Item':                     'item_nbr',
    'Subinventory':             'subinventory',
    'Locator':                  'locator',
    'Transfer Org':             'transfer_org',
    'Owning Party':             'owning_party',
    'From Owning Party':        'from_owning_party',
    'Transaction Date':         'transaction_date',
    'Transaction ID':           'transaction_id',
    'Receiving Transaction ID': 'receiving_transaction_id',
    'Transaction Quantity':     'transaction_qty',
    'Transaction UOM':          'transaction_uom',
    'Primary Quantity':         'primary_qty',
    'Primary UOM':              'primary_uom',
    'Transaction Type':         'transaction_type',
    'Transaction Action':       'transaction_action',
    'Source Type':              'source_type',
    'Source':                   'source',
    'Source Code':              'source_code',
    'Shipment Number':          'shipment_nbr',
    'Waybill/Airbill':          'waybill',
    'Freight Code':             'freight_code',
    'Reason':                   'reason',
}


def _extract_material_transactions(file_prefix: str) -> pd.DataFrame:
    pattern = os.path.join(ORACLE_RAW_DIR, f'{file_prefix}_*.xls')
    files = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(f"No files found matching {file_prefix}_*.xls in {ORACLE_RAW_DIR}")

    dfs = []
    for filepath in files:
        org = os.path.basename(filepath).replace(f'{file_prefix}_', '').replace('.xls', '')
        df = pd.read_csv(filepath, sep='\t', dtype=str, encoding='latin-1')
        df.columns = df.columns.str.strip()

        missing = set(COLUMN_RENAME) - set(df.columns)
        if missing:
            raise ValueError(f"{os.path.basename(filepath)} is missing expected columns: {sorted(missing)}")

        df = df[list(COLUMN_RENAME)].rename(columns=COLUMN_RENAME)
        df['org'] = org
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def extract_castle_oracle_transfer_receipts() -> pd.DataFrame:
    """
    Receiving side: Int Req Intr Rcpt transactions, one file per receiving org
    (transfer_receipts_<ORG>.xls). Source = internal requisition number.
    """
    return _extract_material_transactions('transfer_receipts')


def extract_castle_oracle_transfer_shipments() -> pd.DataFrame:
    """
    Shipping side: Internal order transactions, one file per shipping org
    (transfer_orders_<ORG>.xls). Source = '<SO number>.<order type>.ORDER ENTRY'.
    """
    return _extract_material_transactions('transfer_orders')
