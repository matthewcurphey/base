{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_castle__sales') }}
),

salesinvoice_rows as (
    select
        -- Location
        'castle' as company,
        inv_org_code as org,
        branch_name as branch,

        -- SO
        sales_order_nbr          as so_nbr,
        sales_line_nbr           as so_line,
        shipment_nbr             as shipment_number,
        sales_type,

        -- Invoiced fields
        NULL as invoice_nbr,
        NULL as invoice_line,
        invoice_date as invoiced_date,
        invoiced_lbs,
        invoiced_pcs,
        invoiced_qty,
        invoiced_uom,
        total_sales_usd as invoiced_usd,

        -- Ordered fields
        order_date as ordered_date,
        ordered_lbs,
        ordered_pcs,
        ordered_qty,
        ordered_uom,

        -- Product attributes
        product_item_nbr         as item_number,
        product_primary_item_nbr as primary_item_number,
        product_item_description as item_description,
        product_item_type,
        product_form,
        product_grade,
        product_shape,
        product_temper,
        product_width,
        product_length,
        product_primary_dimension,
        product_stocking_uom,
        product_commodity,
        product_source_type,

        -- Customer
        sold_to_customer_name as customer_name,
        sold_to_customer_nbr as customer_nbr,

        -- Dates
        actual_ship_date,
        promise_date,
        request_date,
        quote_date

    from src
    where
        -- keep only valid sales
        lower(sales_status) = 'valid'

        -- keep only true sales order lines (multi-country variants)
        and lower(line_transaction_type) like 'sales%'

        -- remove crazy high sales lines
        and total_sales_usd < 100000000

)

select *
from salesinvoice_rows