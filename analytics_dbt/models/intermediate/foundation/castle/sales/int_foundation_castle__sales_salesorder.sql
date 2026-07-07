{{ config(materialized='table') }}

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
           INVOICED QUANTITIES
           ======================= */
        max(invoiced_lbs)           as invoiced_lbs,
        max(invoiced_pcs)           as invoiced_pcs,
        max(invoiced_qty)           as invoiced_qty,
        min(invoiced_uom)           as invoiced_uom,

        /* =======================
           WEIGHT
           ======================= */
        max(weight_lbs)             as weight_lbs,
        max(gross_weight_lbs)       as gross_weight_lbs,

        /* =======================
           CUT SIZE
           ======================= */
        max(cut_uom)                     as cut_uom,
        max(cut_shape)                   as cut_shape,
        max(cut_width)                   as cut_width,
        max(cut_length)                  as cut_length,

        /* =======================
           ORDER DATES
           ======================= */
        min(order_date)             as order_date,
        min(promise_date)           as promise_date,
        min(request_date)           as request_date,
        min(quote_date)             as quote_date,
        min(actual_ship_date)       as actual_ship_date,
        min(invoice_date)           as invoice_date,

        /* =======================
           FINANCIALS
           ======================= */
        max(total_sales_usd)                as total_sales_usd,
        max(material_revenue)               as material_revenue,
        max(proc_rev_usd)                   as proc_rev_usd,
        max(freight_revenue_usd)            as freight_revenue_usd,
        max(total_gross_profit_usd)         as total_gross_profit_usd,
        max(matl_gp_usd)                    as matl_gp_usd,
        max(tgp_pct)                        as tgp_pct,
        max(mgp_pct)                        as mgp_pct,
        max(material_cost_aac)              as material_cost_aac,
        max(material_overhead_cost)         as material_overhead_cost,
        max(outside_processing_cost)        as outside_processing_cost,
        max(resource_cost_usd)              as resource_cost_usd,
        max(absorption_cost_usd)            as absorption_cost_usd,
        max(freight_cost)                   as freight_cost,
        max(list_price_per_lbs_gross)       as list_price_per_lbs_gross,
        max(price_per_lbs_gross)            as price_per_lbs_gross,

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
        min(product_customer)                as product_customer,

        /* =======================
           CUSTOMER
           ======================= */
        min(sold_to_customer_name)   as sold_to_customer_name,
        min(sold_to_customer_nbr)    as sold_to_customer_nbr,
        min(ship_to_customer_name)   as ship_to_customer_name,
        min(ship_to_customer_nbr)    as ship_to_customer_nbr

    from src
    where
        -- sales_status/line_transaction_type are NOT filtered here — they
        -- used to be, but that silently hid cancelled orders from every
        -- downstream consumer (including ones that need to see them, like
        -- DJ exception reporting). Consumers that want valid-sales-only
        -- (backlog_daily, hot_components) apply that filter themselves.
        total_sales_usd < 100000000

    group by
        sales_order_nbr,
        sales_line_nbr,
        shipment_nbr,
        product_item_nbr
)

select *
from sales_rows
