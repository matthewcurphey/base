{{ config(materialized='view') }}

with actual_usage as (
    select *
    from {{ ref('int_foundation_banner__mfg_actualusage') }}
),

expected_usage as (
    select *
    from {{ ref('int_foundation_banner__mfg_expectedusageitem') }}
)

select
    /* =======================
       IDENTIFIERS
       ======================= */
    au.company,
    au.production_order_number,
    au.item_number,

    /* =======================
       ACTUAL USAGE
       ======================= */
    au.picked_lbs,
    coalesce(
        au.picked_financial_cost,
        au.picked_physical_cost
    ) as picked_usd,

    /* =======================
       BOM / ENGINEERING CONTEXT
       ======================= */
    eu.expected_yield as expected_yield_item

from actual_usage au

left join expected_usage eu
    on  au.company = eu.company
    and au.production_order_number = eu.production_order_number
    and au.item_number = eu.item_number