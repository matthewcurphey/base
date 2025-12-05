{{ config(materialized='view') }}

-- 1) Pull raw ERP data exactly as-is
with raw as (

    select
        "dataAreaId"     as company,
        "InvoiceId"      as invoice_id,
        "InvoiceDate"    as invoice_date,
        "LineNum"        as line_number,
        "InventQty"      as inventory_quantity,
        "DlvDate"        as delivery_date,
        "Qty"            as quantity,
        "SalesUnit"      as sales_unit,
        "SalesId"        as sales_id,
        "SalesPrice"     as sales_price,
        "ItemId"         as item_number,
        "LineAmount"     as line_amount

    from {{ source('banner', 'banner_invoicelines') }}

),

-- 2) Trim + Normalize Types
cleaned as (

    select
        -- Keys / Identifiers
        lower(trim(company))                    as company,
        trim(invoice_id)                        as invoice_id,
        trim(sales_id)                          as sales_id,
        trim(item_number)                       as item_number,

        -- Line info
        trim(line_number)                       as line_number,
        trim(sales_unit)                        as sales_unit,

        -- Dates (standardised to date)
        cast(invoice_date as date)              as invoice_date,
        cast(delivery_date as date)             as delivery_date,

        -- Quantities & pricing
        cast(inventory_quantity as numeric(18,6)) as inventory_quantity,
        cast(quantity as numeric(18,6))           as quantity,
        cast(sales_price as numeric(18,6))        as sales_price,
        cast(line_amount as numeric(18,2))        as line_amount

    from raw
    where invoice_id is not null
)

select * from cleaned
