{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_banner__productionorders') }}
),

staged as (

    select
        -- Natural keys / identifiers
        company::text                              as company,
        production_order_number::text              as production_order_number,
        item_number::text                          as item_number,
        production_order_status::text              as production_order_status,

        -- Date fields
        started_date::date                         as started_date,
        delivery_date::date                        as delivery_date,
        ended_date::date                           as ended_date,
        source_bom_version_validity_date::date     as source_bom_version_validity_date,

        -- Demand / dependencies
        demand_sales_order_number::text            as demand_sales_order_number,
        demand_sales_order_line_lot_id::text       as demand_sales_order_line_lot_id,
        demand_production_order_line_number::text  as demand_production_order_line_number,

        -- Quantities
        estimated_quantity::numeric(18,6)          as estimated_quantity,
        ordered_sales_quantity::numeric(18,6)       as ordered_sales_quantity,

        -- Joined sales line info
        sales_line_number::text                    as sales_line_number,
        line_amount::numeric(18,2)                 as line_amount,
        sales_unit_symbol::text                    as sales_unit_symbol,

        -- Metadata
        product_size_id::text                      as product_size_id,
        production_site_id::text                   as production_site_id,
        source_bom_id::text                        as source_bom_id

    from base
)

select * from staged