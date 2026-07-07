{{ config(materialized='table') }}

select

    inv_org_code as org,
    branch_name as branch,
    so_nbr,
    so_line as line,
    so_shipment as shp,
    item_clean as item,
    order_status,
    pick_status,
    credit_hold,
    order_date as order_dt,
    invoice_date as invoice_dt,
    ship_to_customer_name as customer,
    dj_nbr,
    job_status as dj_status,
    comp_issued_qty as iss_qty

from {{ ref('int_oracle__mcmaster_exception_to_cancel') }}
