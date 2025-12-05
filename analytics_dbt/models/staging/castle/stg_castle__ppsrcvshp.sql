{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_castle__ppsrcvshp') }}
),

staged as (

    select
        date_value::date                         as date_value,
        operation_code::text                     as operation_code,
        hrs_earned::numeric(18,6)                as hrs_earned,
        org::text                                as org,
        identifier::text                         as identifier,
        year::numeric(18,0)                      as year,
        count_value::numeric(18,0)               as count_value,

        product_form::text                       as product_form,
        product_commodity::text                  as product_commodity,
        product_grade::text                      as product_grade,
        product_item_number::text                as product_item_number,

        lbs::numeric(18,6)                       as lbs,
        qty::numeric(18,6)                       as qty,
        uom::text                                as uom

    from base
)

select * from staged