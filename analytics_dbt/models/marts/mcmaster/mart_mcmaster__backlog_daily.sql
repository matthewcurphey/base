{{ config(materialized='table') }}

select

    dt,
    inv_org_code,
    new_orders,
    shipped_orders,
    open_orders,
    is_weekday,
    is_holiday,
    is_business_day,
    new_orders_week_avg,
    shipped_orders_week_avg,
    backlog_week_avg

from {{ ref('int_castle__mcmaster_metric_backlog_daily') }}
