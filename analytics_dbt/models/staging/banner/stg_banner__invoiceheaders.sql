{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_banner__invoiceheaders') }}
),

staged as (

    select
        -- Natural keys / identifiers
        company::text                  as company,
        invoice_id::text               as invoice_id,
        sales_id::text                 as sales_id,
        invoice_account::text          as invoice_account,
        order_account::text            as order_account,

        -- Financials
        invoice_amount::numeric(18,2)  as invoice_amount,
        currency_code::text            as currency_code,
        quantity::numeric(18,6)        as quantity,

        -- Dates
        invoice_date::date             as invoice_date,

        -- Location
        warehouse::text                as warehouse,
        site_id::text                  as site_id,

        -- Descriptive fields
        delivery_name::text            as delivery_name,
        ledger_voucher::text           as ledger_voucher

    from base
)

select * from staged
