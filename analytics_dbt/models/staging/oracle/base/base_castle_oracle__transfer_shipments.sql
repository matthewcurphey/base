{{ config(materialized='view') }}

with raw as (

    select
        org,
        item_nbr,
        subinventory,
        locator,
        transfer_org,
        transaction_date,
        transaction_id,
        transaction_qty,
        transaction_uom,
        primary_qty,
        primary_uom,
        transaction_type,
        transaction_action,
        source_type,
        source,
        shipment_nbr,
        waybill,
        freight_code

    from {{ source('oracle', 'castle_oracle_transfer_shipments') }}

),

cleaned as (

    select
        trim(org)                           as org,
        trim(item_nbr)                      as item_nbr,
        trim(subinventory)                  as subinventory,
        trim(locator)                       as locator,
        trim(transfer_org)                  as transfer_org,
        trim(transaction_date)              as transaction_date,
        trim(transaction_id)                as transaction_id,
        trim(transaction_qty)               as transaction_qty,
        trim(transaction_uom)               as transaction_uom,
        trim(primary_qty)                   as primary_qty,
        trim(primary_uom)                   as primary_uom,
        trim(transaction_type)              as transaction_type,
        trim(transaction_action)            as transaction_action,
        trim(source_type)                   as source_type,
        trim(source)                        as source,
        trim(shipment_nbr)                  as shipment_nbr,
        trim(waybill)                       as waybill,
        trim(freight_code)                  as freight_code

    from raw

)

select * from cleaned
