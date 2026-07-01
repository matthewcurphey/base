{{ config(materialized='table') }}

with base as (

    select * from {{ ref('int_foundation_castle__sales_production') }}
    where lower(sales_status) = 'valid'
      and lower(line_transaction_type) like 'sales%'
      and sales_type = 'Order'
      and (ship_to_customer_nbr = '4872' or sold_to_customer_nbr = '4872')
      and so_shipment::int > 1

)

/* =====================================================
   McMaster open order lines with shipment nbr > 1.
   These should be cancelled — shipment splits are not
   valid for McMaster and will cause duplicate fulfilment.
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
    sales_type,
    sales_status,
    line_transaction_type,
    order_date,
    promise_date,
    request_date,
    invoice_date,

    /* =======================
       ITEM
       ======================= */
    product_primary_item_nbr,
    product_item_description,
    product_form,
    product_commodity,

    /* =======================
       QUANTITIES
       ======================= */
    ordered_qty,
    ordered_uom,
    invoiced_qty,

    /* =======================
       CUSTOMER
       ======================= */
    ship_to_customer_name,
    ship_to_customer_nbr,

    /* =======================
       DJ
       ======================= */
    dj_nbr,
    dj_org,
    job_status,
    dj_start_date,
    complete_date,
    start_qty,
    complete_qty,
    job_uom,
    comp_item,
    comp_uom,
    comp_qty_per_assy,
    comp_expected_qty,
    comp_issued_qty,
    operation_steps

from base
order by
    inv_org_code,
    so_nbr,
    so_line,
    so_shipment
