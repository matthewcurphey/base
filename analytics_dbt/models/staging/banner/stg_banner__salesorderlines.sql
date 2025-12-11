{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_banner__salesorderlines') }}
),

staged as (

    select
        -- Identifiers
        company::text                          as company,
        sales_order_number::text               as sales_order_number,
        item_number::text                      as item_number,
        line_number::text                      as line_number,
        customers_line_number::text            as customers_line_number,
        inventory_lot_id::text                 as inventory_lot_id,

        -- Dates
        requested_receipt_date::date           as requested_receipt_date,
        requested_shipping_date::date          as requested_shipping_date,

        -- Quantities & amounts
        ordered_sales_quantity::numeric(18,6)  as ordered_sales_quantity,
        sales_price::numeric(18,6)             as sales_price,
        line_amount::numeric(18,2)             as line_amount,

        -- Metadata
        sales_order_line_status::text          as sales_order_line_status,
        line_description::text                 as line_description,
        product_grade::text                  as product_grade,
        currency_code::text                    as currency_code,

        -- Logistics
        sales_unit_symbol::text                as sales_unit_symbol,
        shipping_site_id::text                 as shipping_site_id,
        shipping_warehouse_id::text            as shipping_warehouse_id,
        delivery_address_description::text     as delivery_address_description,
        delivery_mode_code::text               as delivery_mode_code

    from base
)

select * from staged