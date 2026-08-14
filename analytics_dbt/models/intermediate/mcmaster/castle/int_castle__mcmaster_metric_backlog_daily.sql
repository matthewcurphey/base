{{ config(materialized='table') }}

with mcm_sales as (

    -- Filtered to McMaster, shipment 1 only, valid sales lines only —
    -- a backlog trend shouldn't count cancelled orders. This filter used
    -- to live in the foundation model; it's applied here explicitly now
    -- since other consumers of that model need to see cancelled rows.
    -- Also drops orders loaded with a bad 12/18/2026 promise date — excluded
    -- from the report entirely, not just flagged. IS DISTINCT FROM (not <>)
    -- so a null promise_date isn't also filtered out.
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
      and promise_date is distinct from '2026-12-18'

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
   BUSINESS-DAY FLAG
   is_business_day = a real weekday that isn't a designated holiday
   (ref_holidays) — the only thing that determines whether a day gets
   its own verdict downstream (mart_mcmaster__daily_target_performance)
   and whether it counts toward the week-average's divisor below. A
   holiday landing on a weekday is NOT a business day even though
   is_weekday is still true for it — a real holiday and a lazy Tuesday
   with zero orders need to read differently, which bare is_weekday
   can't distinguish.
===================================================== */

qualified as (

    select
        d.*,
        extract(dow from d.dt) not in (0, 6)                        as is_weekday,
        h.holiday_date is not null                                  as is_holiday,
        extract(dow from d.dt) not in (0, 6) and h.holiday_date is null
                                                                     as is_business_day
    from daily d
    left join {{ ref('ref_holidays') }} h
        on h.holiday_date = d.dt

),

/* =====================================================
   WEEK AVERAGE
   Trailing 7 CALENDAR days, always — not "the last 5 business days,
   however far back that reaches." Every day's value (weekday, weekend,
   holiday, doesn't matter) goes into the numerator sum untouched; only
   is_business_day days count toward the divisor. This means a lumpy
   week (e.g. most shipments landing on the one day a week the truck
   actually leaves) still averages out sensibly — nothing is dropped
   from the sum, the divisor just narrows on a short week (a holiday in
   the window drops it to, say, 4 instead of 5).

   Backlog is a level, not a flow — going into a weekend with 2,000
   open orders is just true, no day-type qualification needed — so its
   average is a plain mean over the same 7 calendar days, not divided
   by business-day count.
===================================================== */

windowed as (

    select
        dt,
        inv_org_code,
        round(
            sum(new_orders) over w_7d ::numeric
            / nullif(sum(case when is_business_day then 1 else 0 end) over w_7d, 0)
        , 1)                                                        as new_orders_week_avg,
        round(
            sum(shipped_orders) over w_7d ::numeric
            / nullif(sum(case when is_business_day then 1 else 0 end) over w_7d, 0)
        , 1)                                                        as shipped_orders_week_avg,
        round(
            avg(open_orders) over w_7d
        , 1)                                                        as backlog_week_avg
    from qualified
    window w_7d as (partition by inv_org_code order by dt rows between 6 preceding and current row)

)

select
    q.dt,
    q.inv_org_code,
    q.new_orders,
    q.shipped_orders,
    q.open_orders,
    q.is_weekday,
    q.is_holiday,
    q.is_business_day,
    w.new_orders_week_avg,
    w.shipped_orders_week_avg,
    w.backlog_week_avg

from qualified q
left join windowed w
    on  w.dt           = q.dt
    and w.inv_org_code = q.inv_org_code

order by q.dt, q.inv_org_code
