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

enriched as (

    select
        b.*,

        -- =====================================
        -- Numeric Sort Keys (Hidden in PBI)
        -- =====================================

        (b.fiscal_year * 100 + b.fiscal_month) as fiscal_year_month_key,
        (b.fiscal_year * 100 + b.fiscal_week)  as fiscal_year_week_key,

        -- =====================================
        -- Compact Numeric Labels
        -- =====================================

        -- 25-01
        concat(
            right(b.fiscal_year::text, 2),
            '-',
            lpad(b.fiscal_month::text, 2, '0')
        ) as fiscal_yy_mm,

        -- 2025-01
        concat(
            b.fiscal_year::text,
            '-',
            lpad(b.fiscal_month::text, 2, '0')
        ) as fiscal_yyyy_mm,

        -- =====================================
        -- Month Name Labels
        -- =====================================

        -- Jan-25
        concat(
            left(b.month_name, 3),
            '-',
            right(b.fiscal_year::text, 2)
        ) as fiscal_mon_yy,

        -- Jan-2025
        concat(
            left(b.month_name, 3),
            '-',
            b.fiscal_year::text
        ) as fiscal_mon_yyyy,

        -- =====================================
        -- Structural Month Dates
        -- =====================================

        min(b.calendar_date) over (
            partition by b.fiscal_year, b.fiscal_month
        ) as fiscal_month_start_date,

        max(b.calendar_date) over (
            partition by b.fiscal_year, b.fiscal_month
        ) as fiscal_month_end_date,

        -- =====================================
        -- Structural Week Dates
        -- =====================================

        min(b.calendar_date) over (
            partition by b.fiscal_year, b.fiscal_week
        ) as fiscal_week_start_date,

        max(b.calendar_date) over (
            partition by b.fiscal_year, b.fiscal_week
        ) as fiscal_week_end_date

    from base b

)

select * from enriched