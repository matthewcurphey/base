{{ config(materialized='view') }}

with raw as (

    select
        org,
        item_nbr,
        subinventory,
        locator,
        transfer_org,
        owning_party,
        from_owning_party,
        transaction_date,
        transaction_id,
        receiving_transaction_id,
        transaction_qty,
        transaction_uom,
        primary_qty,
        primary_uom,
        transaction_type,
        transaction_action,
        source_type,
        source                              as requisition_nbr,
        source_code,
        shipment_nbr,
        waybill,
        freight_code,
        reason

    from {{ source('oracle', 'castle_oracle_transfer_receipts') }}

),

cleaned as (

    select
        trim(org)                           as org,
        trim(item_nbr)                      as item_nbr,
        trim(subinventory)                  as subinventory,
        trim(locator)                       as locator,
        trim(transfer_org)                  as transfer_org,
        trim(owning_party)                  as owning_party,
        trim(from_owning_party)             as from_owning_party,
        trim(transaction_date)              as transaction_date,
        trim(transaction_id)                as transaction_id,
        trim(receiving_transaction_id)      as receiving_transaction_id,
        trim(transaction_qty)               as transaction_qty,
        trim(transaction_uom)               as transaction_uom,
        trim(primary_qty)                   as primary_qty,
        trim(primary_uom)                   as primary_uom,
        trim(transaction_type)              as transaction_type,
        trim(transaction_action)            as transaction_action,
        trim(source_type)                   as source_type,
        trim(requisition_nbr)               as requisition_nbr,
        trim(source_code)                   as source_code,
        trim(shipment_nbr)                  as shipment_nbr,
        trim(waybill)                       as waybill,
        trim(freight_code)                  as freight_code,
        trim(reason)                        as reason

    from raw

)

select * from cleaned
