{{ config(materialized='table') }}

select

    dt,
    inv_org_code,
    new_orders,
    shipped_orders,
    open_orders,
    is_weekday,
    has_activity,
    new_orders_5d_avg,
    shipped_orders_5d_avg,
    backlog_5d_avg

from {{ ref('int_castle__mcmaster_metric_backlog_daily') }}
