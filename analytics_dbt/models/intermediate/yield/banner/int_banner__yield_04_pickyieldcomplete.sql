{{ config(materialized='view') }}

with pick_yield_order as (
    select *
    from {{ ref('int_banner__yield_03_pickyieldorder') }}
),

produced_qty as (
    select *
    from {{ ref('int_foundation_banner__mfg_producedqty') }}
)

select
    pyo.company as company,
    pyo.production_order_number as production_order_number,
    pyo.picked_items as picked_items,
    pyo.picked_lbs as picked_lbs,
    pyo.picked_usd as picked_usd,
    
    pq.complete_lbs as complete_lbs,
    pq.complete_lbs * (pyo.picked_usd / nullif(pyo.picked_lbs, 0)) as complete_usd,

    pq.complete_lbs / nullif(pyo.expected_yield, 0) as engineered_lbs,
    pq.complete_lbs / nullif(pyo.expected_yield, 0) * (pyo.picked_usd / nullif(pyo.picked_lbs, 0)) as engineered_usd,

    pyo.expected_yield as expected_yield,
    pq.complete_lbs / nullif(pyo.picked_lbs, 0) as actual_yield

from pick_yield_order pyo
left join produced_qty pq
    on pyo.company = pq.company
    and pyo.production_order_number = pq.production_order_number
