{{ config(materialized='view') }}

with sales as (
    select
        company,
        shipping_site_id        as branch,
        shipping_site_id        as org,
        sales_order_number      as so_nbr,
        line_number             as so_line,
        item_number             as item_number,
        requested_shipping_date as request_date,
        ordered_sales_quantity  as ordered_qty,
        line_amount             as sales_line_amount,
        sales_order_line_status as sales_type,
        line_description        as item_description,
        product_grade,
        sales_unit_symbol       as ordered_uom,
        shipping_site_id,
        shipping_warehouse_id,
        delivery_address_description as customer_name,
        delivery_mode_code
    from {{ ref('stg_banner__salesorderlines') }}
    where lower(sales_order_line_status) <> 'canceled'
),

-- ⭐ NEW: aggregated invoice CTE
invoice as (
    select
        company,
        sales_id                   as so_nbr,
        item_number,
        line_number                as invoice_line,

        -- SUM numeric values
        sum(quantity)              as invoiced_qty,
        sum(line_amount)           as invoiced_usd,

        -- choose "max" or "min" here for ship/promise dates (we use MAX)
        max(delivery_date)         as actual_ship_date,
        max(invoice_date)          as invoiced_date,

        -- UOM – if inconsistent, max() is safe deterministic choice
        max(sales_unit)            as invoiced_uom,

        -- concat all distinct invoice IDs
        string_agg(distinct invoice_id::text, ', ') as invoice_nbr,

        -- keep raw invoice line numbers (not used in final join)
        string_agg(distinct line_number::text, ', ') as raw_invoice_lines

    from {{ ref('stg_banner__invoicelines') }}
    where sales_id is not null
      and trim(sales_id) <> ''
    group by
        company,
        sales_id,
        item_number,
        line_number
)

select
    -- Location
    s.company,
    s.org,
    s.branch,

    -- SO
    s.so_nbr,
    s.so_line,
    NULL AS shipment_number,
    s.sales_type,

    -- Invoiced fields (all preserved exactly as before)
    i.invoice_nbr,
    i.invoice_line,
    i.invoiced_date,
    NULL::numeric(18,4) as invoiced_lbs,
    NULL::numeric(18,4) as invoiced_pcs,
    i.invoiced_qty,
    i.invoiced_uom,
    i.invoiced_usd,

    -- Ordered fields
    NULL::date as ordered_date,
    NULL::numeric(18,4) as ordered_lbs,
    NULL::numeric(18,4) as ordered_pcs,
    s.ordered_qty,
    s.ordered_uom,

    -- Product attributes
    s.item_number,
    NULL as primary_item_number,
    s.item_description,
    NULL AS product_item_type,
    NULL AS product_form,
    s.product_grade,
    NULL AS product_shape,
    NULL AS product_temper,
    NULL::numeric(18,6) AS product_width,
    NULL::numeric(18,6) AS product_length,
    NULL::numeric(18,6) AS product_primary_dimension,
    NULL AS product_stocking_uom,
    NULL AS product_commodity,
    NULL AS product_source_type,

    -- Customer
    s.customer_name,
    NULL AS customer_nbr,

    -- Dates
    i.actual_ship_date,
    NULL::date AS promise_date,
    s.request_date,
    NULL::date AS quote_date

from sales s
left join invoice i
 on s.so_nbr    = i.so_nbr
 and s.so_line  = i.invoice_line
 and s.item_number = i.item_number
