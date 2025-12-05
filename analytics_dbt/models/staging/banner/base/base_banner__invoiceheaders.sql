{{ config(materialized='view') }}

-- 1) Pull raw ERP data exactly as-is
with raw as (

    select
        "dataAreaId"         as company,
        "InvoiceAccount"     as invoice_account,
        "InvoiceAmount"      as invoice_amount,
        "OrderAccount"       as order_account,
        "inventLocationId"   as warehouse,
        "InvoiceDate"        as invoice_date,
        "CurrencyCode"       as currency_code,
        "Qty"                as quantity,
        "InvoiceId"          as invoice_id,
        "SalesId"            as sales_id,
        "PrintMgmtSiteId"    as site_id,
        "DeliveryName"       as delivery_name,
        "LedgerVoucher"      as ledger_voucher

    from {{ source('banner', 'banner_invoiceheaders') }}

),

-- 2) Trim + Normalize Types
cleaned as (

    select
        -- Identifiers
        lower(trim(company))                  as company,
        trim(invoice_id)                      as invoice_id,
        trim(sales_id)                        as sales_id,
        trim(invoice_account)                 as invoice_account,
        trim(order_account)                   as order_account,

        -- Location / Site
        trim(warehouse)                       as warehouse,
        trim(site_id)                         as site_id,

        -- Financials
        cast(invoice_amount as numeric(18,2)) as invoice_amount,
        trim(currency_code)                   as currency_code,
        cast(quantity as numeric(18,6))       as quantity,

        -- Dates
        cast(invoice_date as date)            as invoice_date,

        -- Descriptive
        trim(delivery_name)                   as delivery_name,
        trim(ledger_voucher)                  as ledger_voucher

    from raw
    where invoice_id is not null
)

select * from cleaned
