{{ config(materialized='table') }}

with base as (

    select * from {{ ref('int_oracle__mcmaster_01_sales_production') }}
    where is_mcmaster
      and so_shipment::int > 1
      -- Orders loaded with a bad 12/18/2026 promise date — excluded from
      -- the report entirely, not just flagged. IS DISTINCT FROM (not <>)
      -- so a null promise_date isn't also filtered out.
      and promise_date is distinct from '2026-12-18'

)

/* =====================================================
   McMaster open order lines with shipment nbr > 1.
   Excluded from int_oracle__mcmaster_02_open_backlog —
   shipment splits are not valid for McMaster and cause
   duplicate fulfilment. This is where they surface instead.
===================================================== */

select

    /* =======================
       IDENTIFIERS
       ======================= */
    inv_org_code,
    branch_name,
    so_nbr,
    so_line,
    so_shipment,

    /* =======================
       SO STATUS
       ======================= */
    order_status,
    pick_status,
    credit_hold,
    order_date,
    promise_date,
    request_date,
    invoice_date,

    /* =======================
       ITEM
       ======================= */
    item_clean,
    product_item_description,
    product_form,
    product_commodity,

    /* =======================
       QUANTITIES
       ======================= */
    ordered_qty,
    ordered_uom,

    /* =======================
       CUSTOMER
       ======================= */
    ship_to_customer_name,
    customer_po_number,

    /* =======================
       DJ
       ======================= */
    dj_nbr,
    job_status,
    task_status,
    comp_expected_qty,
    comp_issued_qty

from base
order by
    inv_org_code,
    so_nbr,
    so_line,
    so_shipment
