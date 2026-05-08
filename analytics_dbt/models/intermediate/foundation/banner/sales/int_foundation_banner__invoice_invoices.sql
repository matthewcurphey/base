{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_banner__invoicelines') }}
),

invoice_rows as (
    select
        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        company,
        sales_id           as sales_order_number,
        line_number,
        item_number,

        /* =======================
           INVOICED QUANTITIES & VALUE
           ======================= */
        sum(quantity)      as invoiced_quantity,
        sum(line_amount)   as invoiced_amount,

        /* =======================
           PRICE / UOM
           ======================= */
        max(sales_price)   as invoiced_sales_price,
        min(sales_unit)    as invoiced_uom,

        /* =======================
           DATES
           ======================= */
        max(invoice_date)  as last_invoice_date,
        max(delivery_date) as last_delivery_date,

        /* =======================
           TRACEABILITY
           ======================= */
        string_agg(distinct invoice_id, ',') as invoice_ids

    from src
    where sales_id is not null
      and trim(sales_id) <> ''
    group by
        company,
        sales_id,
        line_number,
        item_number
)

select *
from invoice_rows
