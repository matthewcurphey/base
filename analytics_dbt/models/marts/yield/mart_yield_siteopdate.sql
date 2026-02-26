{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('mart_yield_job') }}

)

select
    -- Org
    company,
    vertical,
    country,
    region,
    org_name,

    -- Time
    complete_date,
    fiscal_year,
    fiscal_month,
    fiscal_week,
    fiscal_year_month_key,
    fiscal_year_month_label,
    fiscal_month_start_date,
    fiscal_year_week_key,
    fiscal_year_week_label,
    fiscal_week_start_date,

    -- Operation
    yield_op_name as yield_op,

    -- Snapshot metadata
    pipeline_run_date,

    -- Trailing flags (pass through)
    is_trailing_1wk,
    is_trailing_2wk,
    is_trailing_4wk,
    is_trailing_8wk,
    is_trailing_12wk,
    is_trailing_26wk,
    is_ttm,

    -- Aggregations
    sum(picked_lbs)     as picked_lbs,
    sum(picked_usd)     as picked_usd,
    sum(complete_lbs)   as complete_lbs,
    sum(complete_usd)   as complete_usd,
    sum(engineered_lbs) as engineered_lbs,
    sum(engineered_usd) as engineered_usd,
    sum(yieldloss_lbs)  as yieldloss_lbs,
    sum(yieldloss_usd)  as yieldloss_usd,
    sum(yieldvar_lbs)   as yieldvar_lbs,
    sum(yieldvar_usd)   as yieldvar_usd

from base

group by
    company,
    vertical,
    country,
    region,
    org_name,

    complete_date,
    fiscal_year,
    fiscal_month,
    fiscal_week,
    fiscal_year_month_key,
    fiscal_year_month_label,
    fiscal_month_start_date,
    fiscal_year_week_key,
    fiscal_year_week_label,
    fiscal_week_start_date,

    yield_op_name,
    pipeline_run_date,

    -- trailing flags
    is_trailing_1wk,
    is_trailing_2wk,
    is_trailing_4wk,
    is_trailing_8wk,
    is_trailing_12wk,
    is_trailing_26wk,
    is_ttm