{{ config(materialized='table') }}

with base as (

    select
        date as calendar_date,
        year as fiscal_year,
        month as fiscal_month,
        week_of_year as fiscal_week,
        week_of_month,
        month_name
    from {{ ref('ref_calendar445') }}

),

anchor as (
    select current_date as run_date
),

current_context as (

    select
        b.fiscal_year as current_fiscal_year,
        b.fiscal_month as current_fiscal_month,
        b.fiscal_week as current_fiscal_week,
        a.run_date
    from base b
    join anchor a on b.calendar_date = a.run_date
)

select
    b.*,

    -- Current Flags
    case when b.calendar_date = c.run_date
         then true else false end as is_today,

    case when b.fiscal_year = c.current_fiscal_year
         then true else false end as is_current_fiscal_year,

    case when b.fiscal_year = c.current_fiscal_year
          and b.fiscal_month = c.current_fiscal_month
         then true else false end as is_current_fiscal_month,

    case when b.fiscal_year = c.current_fiscal_year
          and b.fiscal_week = c.current_fiscal_week
         then true else false end as is_current_fiscal_week,

    -- MTD
    case when b.fiscal_year = c.current_fiscal_year
          and b.fiscal_month = c.current_fiscal_month
          and b.calendar_date <= c.run_date
         then true else false end as is_mtd,

    -- YTD
    case when b.fiscal_year = c.current_fiscal_year
          and b.calendar_date <= c.run_date
         then true else false end as is_ytd,

    -- Trailing 1 Week
    case when b.fiscal_year = c.current_fiscal_year
          and b.fiscal_week = c.current_fiscal_week
         then true else false end as is_trailing_1wk,

    -- Trailing 4 Weeks
    case when b.fiscal_year = c.current_fiscal_year
          and b.fiscal_week between c.current_fiscal_week - 3
                                 and c.current_fiscal_week
         then true else false end as is_trailing_4wk,

    -- Trailing 8 Weeks
    case when b.fiscal_year = c.current_fiscal_year
          and b.fiscal_week between c.current_fiscal_week - 7
                                 and c.current_fiscal_week
         then true else false end as is_trailing_8wk,

    -- Trailing 12 Weeks
    case when b.fiscal_year = c.current_fiscal_year
          and b.fiscal_week between c.current_fiscal_week - 11
                                 and c.current_fiscal_week
         then true else false end as is_trailing_12wk,

    -- TTM (Trailing 12 Months)
    case when b.calendar_date between (c.run_date - interval '12 months')
                                 and c.run_date
         then true else false end as is_ttm

from base b
cross join current_context c