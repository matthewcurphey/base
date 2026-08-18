{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_castle__sales') }}
),

invoice_rows as (
    select
        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        'castle'                    as company,
        sales_order_nbr             as so_nbr,
        sales_line_nbr              as so_line,
        shipment_nbr                as shipment_nbr,
        product_item_nbr            as item_nbr,

        /* =======================
           LOCATION
           ======================= */
        min(inv_org_code)           as inv_org_code,
        min(branch_name)            as branch_name,

        /* =======================
           INVOICE DATES
           ======================= */
        min(invoice_date)           as invoice_date,
        min(actual_ship_date)       as actual_ship_date,

        /* =======================
           INVOICED QUANTITIES
           ======================= */
        sum(invoiced_lbs)           as invoiced_lbs,
        sum(invoiced_pcs)           as invoiced_pcs,
        sum(invoiced_qty)           as invoiced_qty,
        min(invoiced_uom)           as invoiced_uom,

        /* =======================
           REVENUE
           ======================= */
        sum(total_sales_usd)        as invoiced_sales_usd

    from src
    where
        lower(sales_status) = 'valid'
        and lower(line_transaction_type) like 'sales%'
        and total_sales_usd < 100000000

    group by
        sales_order_nbr,
        sales_line_nbr,
        shipment_nbr,
        product_item_nbr
)

select *
from invoice_rows
