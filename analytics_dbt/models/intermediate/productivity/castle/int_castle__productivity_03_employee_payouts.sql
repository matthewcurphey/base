{{ config(materialized='table') }}

-- Castle employee-level worked hours with per-employee bonus calculation
-- Mirrors the month_employee_payout_df logic from productivity.py:
--   month_bonus = total_hrs * payout_usd (org-level rate from productivity results)
-- TEMP employees (employee_name = 'TEMP') get month_bonus = 0, matching Python behaviour

with worked as (

    select *
    from {{ ref('stg_hr__worked_hrs') }}

),

results as (

    select
        org,
        year,
        month,
        payout_usd

    from {{ ref('int_castle__productivity_02_results') }}

)

select
    w.country,
    w.org,
    w.year,
    w.month,
    w.employee_id,
    w.employee_name,
    w.dept_code,
    w.regular_hrs,
    w.overtime_hrs,
    w.total_hrs,
    r.payout_usd,

    case
        when w.employee_name = 'TEMP' then 0
        else w.total_hrs * coalesce(r.payout_usd, 0)
    end as month_bonus

from worked w

left join results r
    on  w.org   = r.org
    and w.year  = r.year
    and w.month = r.month
