{{ config(materialized='view') }}

-- 1) Pull raw ERP data exactly as-is
with raw as (

    select
        "VoucherPhysical"              as physical_voucher,
        "StatusReceipt"                as receipt_status,
        "StatusIssue"                  as issue_status,
        "ReturnInventTransOrigin"      as return_lot_id,
        "Qty"                          as quantity,
        "PackingSlipId"                as packing_slip,
        "OriginReferenceId"            as ref_id,
        "OriginReferenceCategory"      as reference,
        "MarkingRefInventTransOrigin"  as reference_lot,
        "ItemId"                       as item_number,
        "InvoiceId"                    as invoice,
        "InventTransRecId"             as record_id,
        "InventTransOriginInventTransId"   as lot_id,
        "InventTransOriginIItemInventDimId" as dimension_number,
        "InventTransOrigin"            as lot_id2,
        "InventSiteId"                 as site,
        "InventLocationId"             as warehouse,
        "inventDimId"                  as dimension_number2,
        "FinancialVoucher"             as financial_voucher,
        "DatePhysical"                 as physical_date,
        "DateFinancial"                as financial_date,
        "DateClosed"                   as financially_closed,
        "dataAreaId"                   as company,
        "CostAmountPosted"             as financial_cost_amount,
        "CostAmountPhysical"           as physical_cost_amount,
        "CostAmountOperations"         as profit_loss_posted_amount,
        "CostAmountAdjustment"         as adjustment,

        -- Dimension info
        "Size"                         as grade,
        "BatchNumber"                  as batch_number

    from {{ source('banner', 'banner_inventorytransactions') }}
),

-- 2) Trim + Normalize Types
cleaned as (

    select
        lower(trim(company))           as company,
        trim(record_id)                as record_id,
        trim(item_number)              as item_number,
        trim(invoice)                  as invoice,
        trim(financial_voucher)        as financial_voucher,
        trim(packing_slip)             as packing_slip,
        trim(physical_voucher)         as physical_voucher,

        -- Status
        trim(receipt_status)           as receipt_status,
        trim(issue_status)             as issue_status,

        trim(return_lot_id)            as return_lot_id,
        trim(reference)                as reference,
        trim(reference_lot)            as reference_lot,
        trim(ref_id)                   as ref_id,

        -- Quantities
        cast(quantity as numeric)      as quantity,

        -- Dates
        cast(physical_date as date) as physical_date,
        cast(financial_date as date) as financial_date,
        cast(financially_closed as date) as financially_closed,

        -- Costs
        cast(financial_cost_amount as numeric) as financial_cost_amount,
        cast(physical_cost_amount as numeric)   as physical_cost_amount,
        cast(profit_loss_posted_amount as numeric) as profit_loss_posted_amount,
        cast(adjustment as numeric)             as adjustment,

        -- Location
        trim(site)                    as site,
        trim(warehouse)               as warehouse,

        -- Dimensions
        trim(dimension_number)        as dimension_number,
        trim(dimension_number2)       as dimension_number2,
        trim(lot_id)                  as lot_id,
        trim(lot_id2)                 as lot_id2,

        trim(grade)                   as grade,
        trim(batch_number)            as batch_number

    from raw
    where record_id is not null
)

select * from cleaned