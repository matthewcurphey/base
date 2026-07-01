{{ config(materialized='table') }}

with mcm_sales as (

    -- Filtered to McMaster, shipment 1 only. sales_status and
    -- line_transaction_type are already filtered in the foundation.
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

)

select *
from daily
order by dt, inv_org_code
