{{ config(materialized='table') }}


select

    /* =======================
       LANE
       ======================= */
    ship_to_org_code                   as to_org,
    ship_from_org_code || ' -> ' || ship_to_org_code
                                       as lane,

    /* =======================
       TRANSFER
       ======================= */
    so_nbr                             as so_nbr,
    so_line                            as line,
    shipment_nbr                       as shp,
    item_nbr                           as item,
    product_item_description           as item_desc,
    product_form                       as form,
    product_grade                      as grade,
    weight_lbs                         as lbs,
    ordered_qty                        as qty,
    ordered_uom                        as uom,

    /* =======================
       DATES
       ======================= */
    requisition_date                   as req_dt,
    need_by_date                       as need_by_dt,
    promise_date                       as prom_dt,
    ship_date                          as ship_dt,
    received_date                      as rcvd_dt,

    /* =======================
       DURATIONS (days)
       ======================= */
    days_in_transit                    as transit_days,
    days_in_transit_open               as open_transit_days,
    days_requisition_to_received       as req_to_rcvd_days,
    days_received_vs_need_by           as rcvd_vs_need_by_days,

    /* =======================
       ORACLE REFERENCES
       ======================= */
    delivery_nbrs                      as delivery,
    requisition_nbr                    as req_nbr,
    waybill                            as waybill,
    is_shared_delivery                 as shared_delivery,

    /* =======================
       STATUS
       ======================= */
    transfer_status                    as status

from {{ ref('int_oracle__mex_transfers_02_transferline') }}
where ship_from_org_code = 'LOS'
  and transfer_status <> 'Cancelled'
