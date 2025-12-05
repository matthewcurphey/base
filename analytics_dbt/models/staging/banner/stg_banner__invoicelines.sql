{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_banner__invoicelines') }}
),

staged as (

    select
        -- Natural keys
        company::text                        as company,
        invoice_id::text                     as invoice_id,
        sales_id::text                       as sales_id,
        item_number::text                    as item_number,
        line_number::text                    as line_number,

        -- Date fields
        invoice_date::date                   as invoice_date,
        delivery_date::date                  as delivery_date,

        -- Quantities & pricing
        inventory_quantity::numeric(18,6)    as inventory_quantity,
        quantity::numeric(18,6)              as quantity,
        sales_price::numeric(18,6)           as sales_price,
        line_amount::numeric(18,2)           as line_amount,

        -- Units
        sales_unit::text                     as sales_unit

    from base
)

select * from staged
