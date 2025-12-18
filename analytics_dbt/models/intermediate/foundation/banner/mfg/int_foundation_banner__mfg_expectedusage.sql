{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_banner__bom') }}
),

-- 1) Roll up to production order × operation level
bom_rows as (
    select
        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        company,
        production_order_number,
        item_number,


        max(bom_line_quantity)               as bom_line_quantity,
        max(bom_line_quantity_denominator)                as bom_line_quantity_denominator,


        sum(estimated_bom_line_quantity)                  as expected_usage_engineered,
        sum(started_bom_line_quantity)      as expected_usage_started


    from src
    where
         grade is not null
         and trim(grade)<>''
    group by
        company,
        production_order_number,
        item_number
)

select *
from bom_rows
