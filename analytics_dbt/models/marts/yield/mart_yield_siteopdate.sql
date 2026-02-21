{{ config(materialized='view') }}

with yield as (
    select *
    from {{ ref('mart_yield_job') }}
)

select
    company as company,
    vertical as vertical,
    country as country,
    region as region,
    org_name as org_name,
    complete_date as complete_date,
    yield_op_name as yield_op,
    sum(picked_lbs) as picked_lbs,
    sum(picked_usd) as picked_usd,
    sum(complete_lbs) as complete_lbs,
    sum(complete_usd) as complete_usd,
    sum(engineered_lbs) as engineered_lbs,
    sum(engineered_usd) as engineered_usd,
    sum(yieldloss_lbs) as yieldloss_lbs,
    sum(yieldloss_usd) as yieldloss_usd,
    sum(yieldvar_lbs) as yieldvar_lbs,
    sum(yieldvar_usd) as yieldvar_usd

from yield y
group by 
    company,
    vertical,
    country,
    region,
    org_name,
    complete_date,
    yield_op_name  
    