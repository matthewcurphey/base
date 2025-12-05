{{ config(materialized='view') }}

-- 1) Pull raw CSV → Postgres data exactly as loaded
with raw as (

    select
        "Date_Value"                      as date_value,
        "Operation_Code"                  as operation_code,
        "Hrs_Earned"                      as hrs_earned,
        "Org"                             as org,
        "Identifier"                      as identifier,
        "Year"                            as year,
        "Count"                           as count_value,
        "Product_Form"                    as product_form,
        "Product_Commodity"               as product_commodity,
        "Product_Grade"                   as product_grade,
        "Product_Item_Number"             as product_item_number,
        "Lbs"                             as lbs,
        "Qty"                             as qty,
        "UOM"                             as uom

    from {{ source('castle', 'castle_ppsrcvshp') }}
),

-- 2) Clean + normalize
cleaned as (

    select
        cast(date_value as date)                 as date_value,
        trim(operation_code)                     as operation_code,
        cast(hrs_earned as numeric(18,6))        as hrs_earned,
        trim(org)                                as org,
        trim(identifier)                         as identifier,
        cast(year as numeric(18,0))              as year,
        cast(count_value as numeric(18,0))       as count_value,

        trim(product_form)                       as product_form,
        trim(product_commodity)                  as product_commodity,
        trim(product_grade)                      as product_grade,
        trim(product_item_number)                as product_item_number,

        cast(lbs as numeric(18,6))               as lbs,
        cast(qty as numeric(18,6))               as qty,
        trim(uom)                                as uom

    from raw
)

select * from cleaned