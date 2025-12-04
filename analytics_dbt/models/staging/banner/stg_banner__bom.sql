{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_banner__bom') }}
),

staged as (

    select
        -- Natural keys / identifiers
        production_order_number::text            as production_order_number,
        item_number::text                        as item_number,
        line_number::text                        as line_number,
        company::text                            as company,

        -- Quantities: engineering / BOM / production
        bom_line_quantity::numeric(18,6)                 as bom_line_quantity,
        bom_line_quantity_denominator::numeric(18,6)     as bom_line_quantity_denominator,
        started_inventory_quantity::numeric(18,6)        as started_inventory_quantity,
        started_bom_line_quantity::numeric(18,6)         as started_bom_line_quantity,
        remaining_bom_line_quantity::numeric(18,6)       as remaining_bom_line_quantity,
        remaining_inventory_quantity::numeric(18,6)      as remaining_inventory_quantity,
        released_bom_line_quantity::numeric(18,6)        as released_bom_line_quantity,
        estimated_bom_line_quantity::numeric(18,6)       as estimated_bom_line_quantity,

        -- Metadata / relationships
        source_bom_id::text                     as source_bom_id,
        grade::text                   as grade

    from base
)

select * from staged
