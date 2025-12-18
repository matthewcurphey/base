{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_castle__sales') }}
),

sales_rows as (
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
           SALES ORDER METADATA
           ======================= */
        min(sales_status)           as sales_status,
        min(sales_type)             as sales_type,
        min(line_transaction_type)  as line_transaction_type,

        /* =======================
           ORDERED QUANTITIES
           ======================= */
        max(ordered_lbs)            as ordered_lbs,
        max(ordered_pcs)            as ordered_pcs,
        max(ordered_qty)            as ordered_qty,
        min(ordered_uom)            as ordered_uom,

        /* =======================
           ORDER DATES
           ======================= */
        min(order_date)             as order_date,
        min(promise_date)           as promise_date,
        min(request_date)           as request_date,
        min(quote_date)             as quote_date,

        /* =======================
           PRODUCT ATTRIBUTES
           ======================= */
        min(product_primary_item_nbr)        as product_primary_item_nbr,
        min(product_item_description)        as product_item_description,
        min(product_item_type)               as product_item_type,
        min(product_form)                    as product_form,
        min(product_grade)                   as product_grade,
        min(product_shape)                   as product_shape,
        min(product_temper)                  as product_temper,
        min(product_width)                   as product_width,
        min(product_length)                  as product_length,
        min(product_primary_dimension)       as product_primary_dimension,
        min(product_stocking_uom)            as product_stocking_uom,
        min(product_commodity)               as product_commodity,
        min(product_source_type)             as product_source_type,

        /* =======================
           CUSTOMER
           ======================= */
        min(sold_to_customer_name)   as sold_to_customer_name,
        min(sold_to_customer_nbr)    as sold_to_customer_nbr,
        min(ship_to_customer_name)   as ship_to_customer_name,
        min(ship_to_customer_nbr)    as ship_to_customer_nbr

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
from sales_rows
