{{ config(materialized='view') }}

with yield as (
    select *
    from {{ ref('int_banner__yield_06_yieldsiteopdate') }}
)

select
    y.company as company,
    y.country as country,
    y.vertical as vertical,
    y.region as region,
    
    y.org_code as org_code,
    y.org_name as org_name,

    y.production_order_number as production_order_number,

    y.item as item,
    y.grade as grade,
    
    y.prod_status as prod_status,

    y.complete_date as complete_date,
    y.complete_year as complete_year,
    y.complete_month as complete_month,

    y.op_ids as op_ids,
    y.op_names as op_names,

    y.yield_op_id as yield_op_id,
    y.yield_op_name as yield_op_name,

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
