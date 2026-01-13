{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_banner__bom') }}
)

select
    company,
    production_order_number,

    /* total distinct BOM items */
    count(distinct item_number) as bom_item_count,

    /* distinct yield-defining BOM items */
    count(distinct case
        when bom_line_quantity > 0
             and bom_line_quantity_denominator > 0
        then item_number
    end) as valid_bom_item_count

from src
where
    grade is not null
    and trim(grade) <> ''
group by
    company,
    production_order_number