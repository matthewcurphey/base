{{ config(materialized='table') }}

with yield as (
    select *
    from {{ ref('int_banner__yield_04_pickyieldcomplete') }}
),

prod as (
    select *
    from {{ ref('int_foundation_banner__mfg_prodorder') }}
)

select
    y.company as company,
    p.production_site_id as org_code,

    y.production_order_number as production_order_number,

    p.item_number as item,
    p.grade as grade,
    
    p.production_order_status as prod_status,
    p.ended_date as complete_date,

    y.picked_items as picked_items,

    y.picked_lbs as picked_lbs,
    y.picked_usd as picked_usd,
    
    y.complete_lbs as complete_lbs,
    y.complete_usd as complete_usd,

    y.engineered_lbs as engineered_lbs,
    y.engineered_usd as engineered_usd,

    y.yieldloss_lbs as yieldloss_lbs,
    y.yieldloss_usd as yieldloss_usd,

    y.yieldvar_lbs as yieldvar_lbs,
    y.yieldvar_usd as yieldvar_usd,

    y.engineered_yield as engineered_yield,
    y.actual_yield as actual_yield,

    y.yield_performance as yield_performance

from yield y
left join prod p
    on y.company = p.company
    and y.production_order_number = p.production_order_number