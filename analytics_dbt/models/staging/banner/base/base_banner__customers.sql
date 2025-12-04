{{ config(materialized='view') }}

-- 1) Pull raw ERP data exactly as-is
with raw as (

    select
        "dataAreaId",
        "CustomerAccount",
        "PrimaryContactEmail",
        "PrimaryContactPhone",
        "OrganizationName",
        "CreditLimit",
        "SiteId",
        "NameAlias",
        "CustomerGroupId",
        "CredManAccountStatusId",
        "SalesSegmentId",

        -- Delivery Address
        "DeliveryAddressDescription",
        "DeliveryAddressStreet",
        "DeliveryAddressCity",
        "DeliveryAddressCounty",
        "DeliveryAddressState",
        "DeliveryAddressZipCode",
        "DeliveryAddressCountryRegionId",

        -- Primary Address
        "AddressDescription",
        "FullPrimaryAddress",
        "AddressStreet",
        "AddressCity",
        "AddressCounty",
        "AddressState",
        "AddressZipCode",
        "AddressCountryRegionId"

    from {{ source('banner', 'banner_customers') }}
),

-- 2) Normalise columns, fix types, trim junk
cleaned as (

    select
        -- Keys (standardised)
        lower(trim("dataAreaId"))                    as company,
        trim("CustomerAccount")                     as customer_account,

        -- Contacts
        lower(trim("PrimaryContactEmail"))           as email,
        trim("PrimaryContactPhone")                  as phone,

        -- Business attributes
        trim("OrganizationName")                     as customer_name,
        cast("CreditLimit" as numeric)               as credit_limit,
        trim("SiteId")                               as site_id,
        trim("NameAlias")                            as name_alias,
        trim("CustomerGroupId")                      as customer_group_id,
        trim("CredManAccountStatusId")               as credit_status_id,
        trim("SalesSegmentId")                       as sales_segment_id,

        -- Delivery address
        trim("DeliveryAddressDescription")           as delivery_address_description,
        trim("DeliveryAddressStreet")                as delivery_address_street,
        trim("DeliveryAddressCity")                  as delivery_address_city,
        trim("DeliveryAddressCounty")                as delivery_address_county,
        trim("DeliveryAddressState")                 as delivery_address_state,
        trim("DeliveryAddressZipCode")               as delivery_address_zipcode,
        trim("DeliveryAddressCountryRegionId")       as delivery_address_country,

        -- Primary address
        trim("AddressDescription")                   as address_description,
        trim("FullPrimaryAddress")                   as full_primary_address,
        trim("AddressStreet")                        as address_street,
        trim("AddressCity")                          as address_city,
        trim("AddressCounty")                        as address_county,
        trim("AddressState")                         as address_state,
        trim("AddressZipCode")                       as address_zipcode,
        trim("AddressCountryRegionId")               as address_country

    from raw
    where "CustomerAccount" is not null

)

select * from cleaned
