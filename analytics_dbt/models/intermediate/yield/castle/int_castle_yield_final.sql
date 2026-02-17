{{ config(materialized='view') }}

with yield as (
    select *
    from {{ ref('int_castle__yield_02_expectedactualyield') }}
),

operations as (
    select *
    from {{ ref('int_castle_yield_routeattribution') }}
)

select
    y.company as company,
    pyo.production_order_number as production_order_number,
    pyo.picked_items as picked_items,
    pyo.picked_lbs as picked_lbs,
    pyo.picked_usd as picked_usd,
    
    pq.complete_lbs as complete_lbs,
    pq.complete_lbs * (pyo.picked_usd / nullif(pyo.picked_lbs, 0)) as complete_usd,

    pq.complete_lbs / nullif(pyo.expected_yield, 0) as engineered_lbs,
    pq.complete_lbs / nullif(pyo.expected_yield, 0) * (pyo.picked_usd / nullif(pyo.picked_lbs, 0)) as engineered_usd,

    pyo.expected_yield as engineered_yield,
    pq.complete_lbs / nullif(pyo.picked_lbs, 0) as actual_yield

from yield y
left join operations o
    on y.dj_nbr = o.dj_nbr

NEXT STEPS

need to sort banner production order metadata - dates, warehouse, item etc and left join it
probably need to also bring in location data there as well

uniform the columns in both using these types of statements:
null::numeric as expected_weight
'Banner'::text as company

create the union query in the mart
