{{ config(materialized='table') }}

with mcm_sales as (

    -- Filtered to McMaster, shipment 1 only, valid sales lines only —
    -- a backlog trend shouldn't count cancelled orders. This filter used
    -- to live in the foundation model; it's applied here explicitly now
    -- since other consumers of that model need to see cancelled rows.
    select distinct
        inv_org_code,
        so_nbr,
        so_line,
        shipment_nbr,
        order_date,
        invoice_date
    from {{ ref('int_foundation_castle__sales_salesorder') }}
    where (ship_to_customer_nbr = '4872' or sold_to_customer_nbr = '4872')
      and shipment_nbr::int <= 1
      and lower(sales_status) = 'valid'
      and lower(line_transaction_type) like 'sales%'

),

date_spine as (

    select generate_series(
        (select min(order_date) from mcm_sales),
        current_date - interval '1 day',
        interval '1 day'
    )::date as dt

),

orgs as (

    select distinct inv_org_code from mcm_sales

),

spine as (

    select dt, inv_org_code
    from date_spine
    cross join orgs

),

daily as (

    select

        s.dt,
        s.inv_org_code,

        count(case when m.order_date = s.dt
            then 1 end)                                                         as new_orders,

        count(case when m.invoice_date = s.dt
            then 1 end)                                                         as shipped_orders,

        count(case when m.order_date <= s.dt
            and (m.invoice_date is null or m.invoice_date > s.dt)
            then 1 end)                                                         as open_orders

    from spine s
    left join mcm_sales m
        on m.inv_org_code = s.inv_org_code
    group by
        s.dt,
        s.inv_org_code

),

/* =====================================================
   MA QUALIFICATION
   A day counts toward the 5-day averages if it's a
   weekday, or if there was real activity (new order or
   shipment) that day — a quiet weekend doesn't tell us
   anything about performance, so it's skipped rather than
   averaged in as a zero. Same qualification drives the
   backlog average too: a no-activity day means backlog is
   unchanged from the day before, so it adds no signal either.
===================================================== */

qualified as (

    select
        *,
        extract(dow from dt) not in (0, 6)          as is_weekday,
        (new_orders > 0 or shipped_orders > 0)      as has_activity
    from daily

),

/* =====================================================
   5-DAY AVERAGES OVER QUALIFYING DAYS ONLY
   Filtering to qualifying rows before windowing means a
   plain "4 preceding" window only ever sees the compressed
   qualifying sequence per org — non-qualifying days are
   skipped, not counted as zero, so the window reaches back
   further when weekends were quiet.
===================================================== */

ma as (

    select
        dt,
        inv_org_code,
        round(avg(new_orders)
            over (partition by inv_org_code order by dt rows between 4 preceding and current row), 1)  as new_orders_5d_avg,
        round(avg(shipped_orders)
            over (partition by inv_org_code order by dt rows between 4 preceding and current row), 1)  as shipped_orders_5d_avg,
        round(avg(open_orders)
            over (partition by inv_org_code order by dt rows between 4 preceding and current row), 1)  as backlog_5d_avg
    from qualified
    where is_weekday or has_activity

)

select
    d.dt,
    d.inv_org_code,
    d.new_orders,
    d.shipped_orders,
    d.open_orders,
    d.is_weekday,
    d.has_activity,
    m.new_orders_5d_avg,
    m.shipped_orders_5d_avg,
    m.backlog_5d_avg

from qualified d
left join ma m
    on  d.dt           = m.dt
    and d.inv_org_code = m.inv_org_code

order by d.dt, d.inv_org_code
