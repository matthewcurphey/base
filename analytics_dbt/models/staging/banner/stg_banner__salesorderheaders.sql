{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_banner__salesorderheaders') }}
),

staged as (

    select
        -- Keys
        company::text                       as company,
        sales_order_number::text            as sales_order_number,

        -- Status
        sales_order_status::text            as sales_order_status,
        sales_order_processing_status::text as sales_order_processing_status,
        sales_order_name::text              as sales_order_name,

        -- Customer / reference
        customer_requisition_number::text   as customer_requisition_number,
        customer_order_reference::text      as customer_order_reference,
        invoice_customer_account_number::text as invoice_customer_account_number,

        -- Financials
        order_total_amount::numeric(18,2)         as order_total_amount,
        order_total_charges_amount::numeric(18,2) as order_total_charges_amount,
        order_total_tax_amount::numeric(18,2)     as order_total_tax_amount,
        currency_code::text                       as currency_code,

        -- Dates
        order_creation_date::date           as order_creation_date,
        requested_receipt_date::date        as requested_receipt_date,

        case 
            when confirmed_receipt_date = '1900-01-01' then null
            else confirmed_receipt_date
        end::date as confirmed_receipt_date,

        requested_shipping_date::date       as requested_shipping_date,
        
        case 
            when confirmed_shipping_date = '1900-01-01' then null
            else confirmed_shipping_date
        end::date as confirmed_shipping_date,

        -- Customer info
        email::text                         as email,

        -- Delivery address
        delivery_address_name::text         as delivery_address_name,
        delivery_address_street::text       as delivery_address_street,
        delivery_address_city::text         as delivery_address_city,
        delivery_address_state::text        as delivery_address_state,
        delivery_address_zipcode::text      as delivery_address_zipcode,
        delivery_address_country::text      as delivery_address_country,

        -- Logistics
        delivery_mode_code::text            as delivery_mode_code,
        default_shipping_site_id::text      as default_shipping_site_id,
        default_shipping_warehouse_id::text as default_shipping_warehouse_id

    from base
)

select * from staged