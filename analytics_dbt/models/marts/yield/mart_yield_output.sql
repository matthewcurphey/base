{{ config(materialized='table') }}

select
    company,
    vertical,
    country,
    region,
    org_name,

    complete_date,
    fiscal_year,
    fiscal_month,
    fiscal_week,

    prod_number,
    op_ids,
    op_names,
    yield_op_id,
    yield_op_name,

    picked_lbs,
    picked_usd,
    complete_lbs,
    complete_usd,
    yieldloss_lbs,
    yieldloss_usd,
    actual_yield

from {{ ref('mart_yield_job') }}
