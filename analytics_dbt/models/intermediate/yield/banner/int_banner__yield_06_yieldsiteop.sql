{{ config(materialized='table') }}

with yield as (
    select *
    from {{ ref('int_banner__yield_05_prodyield') }}
),

operations as (
    select *
    from {{ ref('int_banner_yield_routeattribution') }}
),

org as (
    select *
    from  {{ ref ('ref_orginfo') }}
)



select
    g.org_company as company,
    g.org_country as country,
    g.org_vertical as vertical,
    g.org_region as region,
    
    y.org_code as org_code,
    g.org_name as org_name,

    y.production_order_number as prod_number,

    y.item as item,
    y.grade as grade,

    y.prod_status as prod_status,

    y.complete_date as complete_date,
    
    o.operation_ids as op_ids,
    o.operation_names as op_names,

    o.yield_loss_operation_id as yield_op_id,
    o.yield_loss_operation_name as yield_op_name,

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

left join operations o
    on y.company = o.company
    and y.production_order_number = o.production_order_number

left join org g
    on y.org_code = g.org_code

