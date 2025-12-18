{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_banner__productionorders') }}
),

-- 1) Roll up to discrete production order level
productionorder_rows as (
    select
        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        company                              as company,
        production_order_number                   as production_order_number,

        /* =======================
           PRODUCT ATTRIBUTES
           (expected 1:1 at prodorder level)
           ======================= */
        min(item_number)                           as item_number,
        min(grade)                                 as grade,

        /* =======================
           PRODUCTION ORDER METADATA
           ======================= */
        min(production_site_id)                    as production_site_id,
        min(production_order_status)               as production_order_status,

        min(started_date)                          as started_date,
        min(delivery_date)                         as delivery_date,
        max(ended_date)                            as ended_date,

        min(source_bom_id)                         as source_bom_id,
        min(source_bom_version_validity_date)
                                                   as source_bom_version_validity_date,

        /* =======================
           SALES DEMAND LINKAGE
           ======================= */
        min(demand_sales_order_number)             as so_nbr,
        min(sales_line_number)                     as so_line,
        min(demand_sales_order_line_lot_id)        as so_line_lot_id,
        min(demand_production_order_line_number)
                                                   as demand_prodorder_line,

        min(sales_unit_symbol)                     as sales_uom,
        max(line_amount)                           as sales_line_amount,

        /* =======================
           QUANTITIES
           (Banner semantics preserved)
           ======================= */
        max(estimated_quantity)                    as estimated_quantity,
        max(ordered_sales_quantity)                as ordered_sales_quantity

    from src
    group by
        company,
        production_order_number
)

select *
from productionorder_rows
