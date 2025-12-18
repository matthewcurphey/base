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
        case 
            when started_date = '1900-01-01' then null
            else started_date
        end::date as started_date,  

        case 
            when delivery_date = '1900-01-01' then null
            else delivery_date
        end::date as delivery_date,

        case 
            when ended_date = '1970-01-01' then null
            else ended_date
        end::date as ended_date,

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
        grade::text                      as grade,
        production_site_id::text                   as production_site_id,
        source_bom_id::text                        as source_bom_id

    from base
)

select * from staged