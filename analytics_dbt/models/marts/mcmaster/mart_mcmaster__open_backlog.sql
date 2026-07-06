{{ config(materialized='table') }}


select

    /* =======================
       SALES
       ======================= */
    inv_org_code                       as org,
    branch_name                        as branch,
    so_nbr                             as so_nbr,
    so_line                            as line,
    so_shipment                        as shp,
    ship_to_customer_name              as customer,
    item_nbr                           as item,
    order_date                         as order_dt,
    promise_date                       as prom_dt,
    order_status                       as order_status,
    pick_status                        as pick_status,
    credit_hold                        as credit_hold,
    request_date                       as req_dt,
    customer_po_number                 as cust_po,
    ordered_qty                        as qty,
    ordered_uom                        as uom,
    weight_lbs                         as lbs,
    total_sales_usd                    as usd,
   
    /* =======================
       ITEM
       ======================= */
    
    item_clean                         as item_clean,
    product_form                       as form,
    product_commodity                  as commodity,
    product_grade                      as grade,
    product_temper                     as temper,
    item_thickness                     as thk,
    product_item_description           as item_desc,
    cust_item_number                   as cust_item,

    /* =======================
       UOM
       ======================= */    
     
    unit_of_measure                    as comp_uom,

    /* =======================
       PRODUCTION
       ======================= */

    dj_nbr                             as dj,
    job_status                         as dj_status,
    comp_expected_qty                  as exp_qty,
    comp_issued_qty                    as iss_qty,
    assembly                           as assy,
    assembly_lot_nbrs                  as assy_lots,
    cut_width                          as cut_wid,
    cut_length                         as cut_len,  

    /* =======================
       INVENTORY
       ======================= */

    comp_inv_req                       as comp_req,
    tally_home_org                     as org_inv_tally,
    inv_atl                            as atl_inv,
    inv_cle                            as cle_inv,
    inv_dal                            as dal_inv,
    inv_jvl                            as jvl_inv,
    inv_los                            as los_inv,
    inv_wie                            as wie_inv,

    /* =======================
       CX REFERENCE
       ======================= */
    cx_reason                          as reason,
    cx_vendor                          as vendor,
    cx_po                              as po,
    cx_comments                        as comments,


    /* =======================
       STATUS
       ======================= */

    mcm_status                         as so_status

from {{ ref('int_oracle__mcmaster_02_open_backlog') }}
