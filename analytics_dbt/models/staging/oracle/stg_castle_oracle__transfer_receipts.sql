{{ config(materialized='view') }}

with base as (
    select * from {{ ref('base_castle_oracle__transfer_receipts') }}
),

staged as (

    select

        /* =======================
           IDENTIFIERS
           One row per Oracle material transaction (transaction_id).
           shipment_nbr is the shipping branch's delivery number;
           requisition_nbr is the internal requisition.
           ======================= */
        transaction_id::text                                            as transaction_id,
        receiving_transaction_id::text                                  as receiving_transaction_id,
        requisition_nbr::text                                           as requisition_nbr,
        shipment_nbr::text                                              as shipment_nbr,
        item_nbr::text                                                  as item_nbr,

        /* =======================
           LOCATION
           org = receiving branch (from the export filename);
           transfer_org = shipping branch.
           ======================= */
        org::text                                                       as receiving_org_code,
        transfer_org::text                                              as shipping_org_code,
        subinventory::text                                              as subinventory,
        locator::text                                                   as locator,

        /* =======================
           RECEIPT
           ======================= */
        to_timestamp(transaction_date, 'DD-MON-YYYY HH24:MI:SS')        as received_at,
        to_timestamp(transaction_date, 'DD-MON-YYYY HH24:MI:SS')::date  as received_date,
        transaction_qty::numeric(18,6)                                  as transaction_qty,
        transaction_uom::text                                           as transaction_uom,
        primary_qty::numeric(18,6)                                      as primary_qty,
        primary_uom::text                                               as primary_uom,
        transaction_type::text                                          as transaction_type,
        reason::text                                                    as reason,

        /* =======================
           FREIGHT
           ======================= */
        waybill::text                                                   as waybill,
        freight_code::text                                              as freight_code

    from base

)

select * from staged
