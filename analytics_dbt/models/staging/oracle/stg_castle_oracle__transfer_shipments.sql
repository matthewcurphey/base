{{ config(materialized='view') }}

with base as (
    select * from {{ ref('base_castle_oracle__transfer_shipments') }}
),

staged as (

    select

        /* =======================
           IDENTIFIERS
           One row per Oracle material transaction (transaction_id).
           Source is '<SO number>.<order type>.ORDER ENTRY'.
           shipment_nbr is the delivery number - the same number the
           receiving branch books its receipt against.
           ======================= */
        transaction_id::text                                            as transaction_id,
        split_part(source, '.', 1)::text                                as so_nbr,
        shipment_nbr::text                                              as shipment_nbr,
        item_nbr::text                                                  as item_nbr,

        /* =======================
           LOCATION
           org = shipping branch (from the export filename);
           transfer_org = receiving branch.
           ======================= */
        org::text                                                       as shipping_org_code,
        transfer_org::text                                              as receiving_org_code,
        subinventory::text                                              as subinventory,
        locator::text                                                   as locator,

        /* =======================
           SHIPMENT
           Oracle books shipments as negative quantities - flipped positive.
           ======================= */
        to_timestamp(transaction_date, 'DD-MON-YYYY HH24:MI:SS')        as shipped_at,
        to_timestamp(transaction_date, 'DD-MON-YYYY HH24:MI:SS')::date  as shipped_date,
        abs(transaction_qty::numeric(18,6))                             as shipped_qty,
        transaction_uom::text                                           as shipped_uom,
        transaction_type::text                                          as transaction_type,

        /* =======================
           FREIGHT
           ======================= */
        waybill::text                                                   as waybill,
        freight_code::text                                              as freight_code

    from base

    -- the export also carries Internal Order Pick rows (staging moves inside
    -- the shipping branch) - only the actual intransit shipments are kept
    where transaction_type = 'Int Order Intr Ship'

)

select * from staged
