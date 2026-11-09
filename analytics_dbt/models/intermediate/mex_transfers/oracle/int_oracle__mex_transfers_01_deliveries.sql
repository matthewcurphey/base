{{ config(materialized='table') }}

/*
    One row per internal order x item x delivery shipped from an exported
    shipping branch, with the receiving branch's receipt against that delivery.

    Exact join, no matching logic: the shipping transaction and the receipt both
    carry Oracle's shipment number (delivery). A delivery can carry several
    internal orders of the same item, and the receipt is booked per delivery, so
    received vs shipped is compared at delivery x item level.
*/

with shipments as (
    select
        so_nbr,
        item_nbr,
        shipment_nbr,
        shipping_org_code,
        receiving_org_code,
        min(shipped_date)                                   as shipped_date,
        sum(shipped_qty)                                    as shipped_qty,
        min(shipped_uom)                                    as shipped_uom,
        min(waybill)                                        as waybill,
        min(freight_code)                                   as freight_code
    from {{ ref('stg_castle_oracle__transfer_shipments') }}
    group by 1, 2, 3, 4, 5
),

delivery_shipped as (
    select
        shipment_nbr,
        item_nbr,
        receiving_org_code,
        sum(shipped_qty)                                    as delivery_shipped_qty,
        count(distinct so_nbr)                              as delivery_order_count
    from shipments
    group by 1, 2, 3
),

delivery_received as (
    select
        shipment_nbr,
        item_nbr,
        receiving_org_code,
        min(received_date)                                  as first_received_date,
        max(received_date)                                  as last_received_date,
        sum(transaction_qty)                                as delivery_received_qty,
        min(transaction_uom)                                as received_uom,
        min(requisition_nbr)                                as requisition_nbr
    from {{ ref('stg_castle_oracle__transfer_receipts') }}
    group by 1, 2, 3
),

final as (
    select
        /* =======================
           IDENTIFIERS
           ======================= */
        s.so_nbr || '|' || s.item_nbr || '|' || s.shipment_nbr || '|' || s.receiving_org_code
                                                            as delivery_line_key,
        s.so_nbr,
        s.item_nbr,
        s.shipment_nbr                                      as delivery_nbr,
        r.requisition_nbr,

        /* =======================
           LANE
           ======================= */
        s.shipping_org_code,
        s.receiving_org_code,

        /* =======================
           SHIPPED
           ======================= */
        s.shipped_date,
        s.shipped_qty,
        s.shipped_uom,
        d.delivery_shipped_qty,
        d.delivery_order_count,
        s.waybill,
        s.freight_code,

        /* =======================
           RECEIVED (delivery x item)
           ======================= */
        r.first_received_date,
        r.last_received_date,
        r.delivery_received_qty,
        r.received_uom,

        /* =======================
           STATUS
           ======================= */
        case
            when r.shipment_nbr is null                                         then 'In transit'
            when r.delivery_received_qty < d.delivery_shipped_qty - 0.01        then 'Partially received'
            else 'Received'
        end                                                 as delivery_status,
        r.first_received_date - s.shipped_date              as days_to_first_receipt,
        r.last_received_date - s.shipped_date               as days_to_last_receipt

    from shipments s
    join delivery_shipped d
        on  d.shipment_nbr       = s.shipment_nbr
        and d.item_nbr           = s.item_nbr
        and d.receiving_org_code = s.receiving_org_code
    left join delivery_received r
        on  r.shipment_nbr       = s.shipment_nbr
        and r.item_nbr           = s.item_nbr
        and r.receiving_org_code = s.receiving_org_code
)

select * from final
