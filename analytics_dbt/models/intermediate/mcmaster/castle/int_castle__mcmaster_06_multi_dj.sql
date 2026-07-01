{{ config(materialized='table') }}

with base as (

    select * from {{ ref('int_foundation_castle__sales_production') }}

),

/* =====================================================
   FIND SO/LINE/SHIPMENT COMBOS WITH MORE THAN ONE DJ
   Open McMaster orders only.
===================================================== */

multi_dj_combos as (

    select
        so_nbr,
        so_line,
        so_shipment,
        count(distinct dj_nbr)          as dj_count
    from base
    where (ship_to_customer_nbr = '4872' or sold_to_customer_nbr = '4872')
      and sales_type = 'Order'
      and dj_nbr    is not null
      and job_status not in ('Cancelled', 'Closed', 'Complete')
    group by so_nbr, so_line, so_shipment
    having count(distinct dj_nbr) > 1

)

/* =====================================================
   RETURN ALL ROWS FOR THOSE COMBOS
   One row per DJ, SO columns repeat across each row.
   dj_count shows how many DJs are attached so the
   full extent of the exception is visible at a glance.
===================================================== */

select

    /* =======================
       IDENTIFIERS
       ======================= */
    base.inv_org_code,
    base.branch_name,
    base.so_nbr,
    base.so_line,
    base.so_shipment,

    /* =======================
       SO STATUS
       ======================= */
    base.sales_type,
    base.sales_status,
    base.line_transaction_type,
    base.order_date,
    base.promise_date,
    base.invoice_date,

    /* =======================
       ITEM
       ======================= */
    base.product_primary_item_nbr,
    base.product_item_description,
    base.product_form,
    base.product_commodity,

    /* =======================
       QUANTITIES
       ======================= */
    base.ordered_qty,
    base.ordered_uom,
    base.invoiced_qty,

    /* =======================
       CUSTOMER
       ======================= */
    base.ship_to_customer_name,
    base.ship_to_customer_nbr,

    /* =======================
       DJ — how many are attached?
       ======================= */
    m.dj_count,
    base.dj_nbr,
    base.dj_org,
    base.job_status,
    base.dj_start_date,
    base.complete_date,
    base.start_qty,
    base.complete_qty,
    base.job_uom,
    base.comp_item,
    base.comp_uom,
    base.comp_qty_per_assy,
    base.comp_expected_qty,
    base.comp_issued_qty,
    base.operation_steps

from base
inner join multi_dj_combos m
    on  base.so_nbr      = m.so_nbr
    and base.so_line     = m.so_line
    and base.so_shipment = m.so_shipment

where (base.ship_to_customer_nbr = '4872' or base.sold_to_customer_nbr = '4872')
  and base.job_status not in ('Cancelled', 'Closed', 'Complete')

order by
    base.so_nbr,
    base.so_line,
    base.so_shipment,
    base.dj_nbr
