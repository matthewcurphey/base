{{ config(materialized='view') }}

with base as (
    select * 
    from {{ ref('base_banner__customers') }}
),

staged as (

    select
        -- Natural keys
        customer_account,
        company,

        -- Contact info
        email,
        phone,

        -- Business attributes
        customer_name,
        credit_limit,
        site_id,
        name_alias,
        customer_group_id,
        credit_status_id,
        sales_segment_id,

        -- Delivery address
        delivery_address_description,
        delivery_address_street,
        delivery_address_city,
        delivery_address_county,
        delivery_address_state,
        delivery_address_zipcode,
        delivery_address_country,

        -- Primary address
        address_description,
        full_primary_address,
        address_street,
        address_city,
        address_county,
        address_state,
        address_zipcode,
        address_country

    from base
)

select * from staged
