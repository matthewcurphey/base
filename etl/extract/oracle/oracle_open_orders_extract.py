import os
import re

import pandas as pd

from config.paths import ORACLE_RAW_DIR

COLUMN_RENAME = {
    'Branch Number':        'branch_number',
    'Branch Name':          'branch_name',
    'Shipping Org':         'shipping_org',
    'Order #-Line#':        'order_line',
    'Customer Name':        'customer_name',
    'Customer PO Number':   'customer_po_number',
    'Cust Item Number':     'cust_item_number',
    'Item Number':          'item_number',
    'Ordered Item Desc':    'ordered_item_desc',
    'Ordered Qty':          'ordered_qty',
    'Ordered UOM':          'ordered_uom',
    'LBS Qty':              'lbs_qty',
    'Item Thickness':       'item_thickness',
    'Cut Width':            'cut_width',
    'Cut Length':           'cut_length',
    'Item Grade':           'item_grade',
    'Item Temper':          'item_temper',
    'Ordered Date':         'ordered_date',
    'Request Date':         'request_date',
    'Promise Date':         'promise_date',
    'Scheduled Ship Date':  'scheduled_ship_date',
    'Actual Ship Date':     'actual_ship_date',
    'Order Status':         'order_status',
    'Pick Status':          'pick_status',
    'Discrete Job':         'discrete_job',
    'Discrete Job Status':  'discrete_job_status',
    'Task Status':          'task_status',
    'Credit Hold':          'credit_hold',
    'Days Early-Late':      'days_early_late',
    'On-Time %':            'on_time_pct',
    'COGS':                 'cogs',
    'Sales $':              'sales_usd',
    'Gross Margin':         'gross_margin',
    'Margin %':             'margin_pct',
    'Promo Name':           'promo_name',
    'Required Quantity':    'required_qty',
    'Quantity Issued':      'quantity_issued',
    'Transaction Date':     'transaction_date',
    'Sales Representative': 'sales_representative',
    'Quantity on Hand':     'quantity_on_hand',
}

# Invisible Unicode control characters Oracle embeds in HTML exports
_UNICODE_CONTROLS = re.compile(r'[​-\u200F\u202A-\u202E⁠-⁤﻿]')


def extract_castle_oracle_open_orders() -> pd.DataFrame:
    path = os.path.join(ORACLE_RAW_DIR, 'AMC_Open_Orders_Report.xls')

    # Oracle exports HTML disguised as .xls; table[0] is the report title, table[1] is data
    df = pd.read_html(path, header=0)[1]

    # Strip any whitespace from column names before renaming
    df.columns = df.columns.str.strip()
    df = df.rename(columns=COLUMN_RENAME)

    # Convert all to string and normalise nulls
    df = df.astype(str).replace('nan', None)

    # Strip invisible Unicode control characters Oracle embeds in values
    df = df.apply(lambda col: col.map(
        lambda v: _UNICODE_CONTROLS.sub('', v) if isinstance(v, str) else v
    ))

    return df
