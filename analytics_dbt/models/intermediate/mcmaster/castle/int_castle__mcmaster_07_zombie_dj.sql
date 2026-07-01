{{ config(materialized='table') }}

with base as (

    select * from {{ ref('int_foundation_castle__sales_production') }}

),

dj_counts as (

    select
        so_nbr,
        so_line,
        so_shipment,
        count(distinct dj_nbr)      as dj_count
    from base
    where (ship_to_customer_nbr = '4872' or sold_to_customer_nbr = '4872')
      and sales_type      != 'Order'
      and job_status  not in ('Cancelled', 'Closed', 'Complete')
      and dj_nbr          is not null
    group by so_nbr, so_line, so_shipment

)

/* =====================================================
   ZOMBIE DJs
   Sales order is no longer open (sales_type != 'Order')
   but the attached DJ is still active (not Closed/Complete).
   No sales_status or line_transaction_type filter —
   intentionally wide to catch all dead order states.
   dj_count flags lines where multiple zombies are attached.
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
       SO STATUS — why is it dead?
       ======================= */
    base.sales_type,
    base.sales_status,
    base.line_transaction_type,
    base.invoice_date,
    base.order_date,
    base.promise_date,

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
       DJ — still alive, shouldn't be
       ======================= */
    d.dj_count,
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
inner join dj_counts d
    on  base.so_nbr      = d.so_nbr
    and base.so_line     = d.so_line
    and base.so_shipment = d.so_shipment

where base.dj_nbr is not null
  and (base.ship_to_customer_nbr = '4872' or base.sold_to_customer_nbr = '4872')
  and base.sales_type       != 'Order'
  and base.job_status   not in ('Cancelled', 'Closed', 'Complete')

order by
    base.job_status,
    base.inv_org_code,
    base.so_nbr
