{{ config(materialized='view') }}

-- Aggregates HR worked hours from employee grain to org / year / month grain
-- Mirrors the output_worked_hrs_summary logic from productivity.py

with detail as (

    select *
    from {{ ref('stg_hr__worked_hrs') }}

)

select
    country,
    org,
    year,
    month,
    sum(regular_hrs)  as worked_reg_hrs,
    sum(overtime_hrs) as worked_ot_hrs,
    sum(total_hrs)    as worked_total_hrs

from detail
group by
    country,
    org,
    year,
    month
