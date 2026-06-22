{{ config(materialized='table') }}

select

    /* =====================================================
       SALE
    ===================================================== */
    inv_org_code,
    branch_name,
    so_nbr,
    so_line,
    so_shipment,
    item_nbr,
    sales_type,
    order_date,
    promise_date,
    request_date,
    ordered_qty,
    ordered_uom,
    weight_lbs,
    gross_weight_lbs,
    total_sales_usd,
    is_mcmaster,
    ship_to_customer_name,
    ship_to_customer_nbr,

    /* =====================================================
       ITEM
    ===================================================== */
    product_primary_item_nbr,
    product_item_description,
    product_item_type,
    product_commodity,
    product_form,
    product_width,
    product_length,
    product_primary_dimension,

    /* =====================================================
       PRODUCTION
    ===================================================== */
    cut_shape,
    cut_uom,
    cut_width,
    cut_length,
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
    operation_steps,

   
    /* =====================================================
       INVENTORY & MATERIAL AVAILABILITY
    ===================================================== */
    assembly,
    assembly_lot_nbrs,
    inv_atl,
    inv_cle,
    inv_dal,
    inv_jvl,
    inv_los,
    inv_wie,
    inv_other,
    inv_total,
    inv_item,
    inv_uom,
    comp_inv_req,
    comp_inv_req_uom,
    tally_home_org,
    is_short

from {{ ref('int_castle__mcmaster_02_open_backlog') }}
