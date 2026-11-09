{{ config(materialized='table') }}


select

    /* =======================
       LANE
       ======================= */
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
    weight_lbs                         as lbs,

    /* =======================
       DATES
       ======================= */
    need_by_date                       as need_by_dt,
    ship_date                          as ship_dt,
    days_in_transit_open               as days_since_ship,
    current_date - need_by_date        as days_past_need_by,

    /* =======================
       ORACLE REFERENCES
       ======================= */
    delivery_nbrs                      as delivery,
    waybill                            as waybill,

    /* =======================
       STATUS
       ======================= */
    transfer_status                    as status

from {{ ref('int_oracle__mex_transfers_02_transferline') }}
where ship_from_org_code = 'LOS'
  and transfer_status in ('In transit', 'Partially received')
