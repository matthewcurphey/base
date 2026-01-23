{{ config(materialized='view') }}

with actual_expected_order as (
    select *
    from {{ ref('int_banner__yield_02_actualexpectedorder') }}
)

select
    /* =======================
       IDENTIFIERS
       ======================= */
    company,
    production_order_number,
    string_agg(distinct item_number, ', ') as picked_items,

    /* =======================
       ACTUAL USAGE
       ======================= */
    sum(picked_lbs) as picked_lbs,
    sum(picked_usd) as picked_usd,

    sum(expected_output_lbs) / nullif(sum(picked_lbs), 0) as expected_yield

from actual_expected_order
group by
        company,
        production_order_number



