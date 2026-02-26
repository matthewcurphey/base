{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('int_castle_yield_final') }}

    union all

    select *
    from {{ ref('int_banner_yield_final') }}

),

anchor as (
    select (current_date - interval '1 day')::date as run_date
),

enriched as (

    select
        b.*,
        d.fiscal_year,
        d.fiscal_month,
        d.fiscal_week,
        d.fiscal_year_month_key,
        d.fiscal_year_month_label,
        d.fiscal_month_start_date,
        d.fiscal_year_week_key,
        d.fiscal_year_week_label,
        d.fiscal_week_start_date
    from base b
    left join {{ ref('dim_calendar') }} d
        on b.complete_date = d.calendar_date

)

select
    e.*,
    a.run_date as pipeline_run_date,

    -- Trailing 1 Week (7 days)
    case when e.complete_date between (a.run_date - interval '6 days')
                                   and a.run_date
         then true else false end as is_trailing_1wk,

    -- Trailing 2 Weeks
    case when e.complete_date between (a.run_date - interval '13 days')
                                   and a.run_date
         then true else false end as is_trailing_2wk,

    -- Trailing 4 Weeks
    case when e.complete_date between (a.run_date - interval '27 days')
                                   and a.run_date
         then true else false end as is_trailing_4wk,

    -- Trailing 8 Weeks
    case when e.complete_date between (a.run_date - interval '55 days')
                                   and a.run_date
         then true else false end as is_trailing_8wk,

    -- Trailing 12 Weeks
    case when e.complete_date between (a.run_date - interval '83 days')
                                   and a.run_date
         then true else false end as is_trailing_12wk,

    -- Trailing 26 Weeks
    case when e.complete_date between (a.run_date - interval '181 days')
                                   and a.run_date
         then true else false end as is_trailing_26wk,

    -- Trailing 12 Months
    case when e.complete_date between (a.run_date - interval '12 months')
                                   and a.run_date
         then true else false end as is_ttm

from enriched e
cross join anchor a