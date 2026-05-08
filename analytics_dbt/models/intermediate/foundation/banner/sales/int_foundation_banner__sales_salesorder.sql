{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_banner__salesorderlines') }}
),

sales_rows as (
    select
        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        company,
        sales_order_number,
        line_number,
        item_number,

        /* =======================
           SALES LINE ATTRIBUTES
           ======================= */
        min(customers_line_number)           as customers_line_number,
        min(inventory_lot_id)                as inventory_lot_id,

        min(sales_order_line_status)         as sales_order_line_status,
        min(line_description)                as line_description,
        min(product_grade)                   as product_grade,
        min(currency_code)                   as currency_code,

        /* =======================
           ORDERED QUANTITIES & VALUE
           ======================= */
        max(ordered_sales_quantity)          as ordered_sales_quantity,
        max(sales_price)                     as ordered_sales_price,
        max(line_amount)                     as ordered_line_amount,
        min(sales_unit_symbol)               as ordered_uom,

        /* =======================
           REQUESTED DATES
           ======================= */
        min(requested_receipt_date)          as requested_receipt_date,
        min(requested_shipping_date)         as requested_shipping_date,

        /* =======================
           LOGISTICS
           ======================= */
        min(shipping_site_id)                as shipping_site_id,
        min(shipping_warehouse_id)           as shipping_warehouse_id,
        min(delivery_address_description)    as delivery_customer_name,
        min(delivery_mode_code)              as delivery_mode_code

    from src
    where lower(sales_order_line_status) <> 'canceled'
    group by
        company,
        sales_order_number,
        line_number,
        item_number
)

select *
from sales_rows
