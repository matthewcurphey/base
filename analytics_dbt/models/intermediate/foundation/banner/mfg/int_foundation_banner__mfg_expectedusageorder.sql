{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_banner__bom') }}
),

bom_rows as (
    select
        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        company,
        production_order_number,

        /* =======================
           ENGINEERING INPUTS
           ======================= */
        sum(bom_line_quantity)            as bom_line_quantity,
        sum(bom_line_quantity_denominator) as bom_line_quantity_denominator,

        /* canonical engineered ratio */
        sum(bom_line_quantity_denominator)
            / nullif(sum(bom_line_quantity), 0)
            as expected_yield,

        /* =======================
           SYSTEM CALCS (diagnostic only)
           ======================= */
        sum(estimated_bom_line_quantity)  as expected_usage_engineered,
        sum(started_bom_line_quantity)    as expected_usage_started

    from src
    where
        grade is not null
        and trim(grade) <> ''
    group by
        company,
        production_order_number
)

select *
from bom_rows