{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_castle__inventory') }}
),

staged as (

    select
        inv_org_code::text                        as inv_org_code,
        product_primary_item_nbr::text            as product_primary_item_nbr,
        item_desc::text                           as item_desc,
        product_commodity::text                   as product_commodity,
        product_shape::text                       as product_shape,
        product_form::text                        as product_form,
        product_grade::text                       as product_grade,
        product_temper::text                      as product_temper,
        product_amc_container_spec::text          as product_amc_container_spec,
        product_primary_dimension::text           as product_primary_dimension,

        product_length::numeric(18,6)             as product_length,
        product_width::numeric(18,6)              as product_width,

        product_item_type::text                   as product_item_type,
        sub_inv_code::text                        as sub_inv_code,

        on_hand_units::numeric(18,6)              as on_hand_units,
        product_stocking_uom::text                as product_stocking_uom,

        on_hand_usd::numeric(18,4)                as on_hand_usd,
        lot_nbr::text                              as lot_nbr,

        lot_length::numeric(18,6)                 as lot_length,
        lot_width::numeric(18,6)                  as lot_width,

        heat_nbr::text                             as heat_nbr,

        on_hand_lbs::numeric(18,6)                as on_hand_lbs,
        on_hand_usd_2::numeric(18,4)              as on_hand_usd_2,

        lot_aging_in_days::numeric(18,2)          as lot_aging_in_days,

        lot_creation_date::date                   as lot_creation_date,
        company_origin_date::date                 as company_origin_date,

        company_origin_lot::text                  as company_origin_lot,
        on_hand_material_usd::numeric(18,4)       as on_hand_material_usd,

        warehouse_locator::text                   as warehouse_locator,
        inv_date::date                            as inv_date,

        item_nbr::text                            as item_nbr

    from base
)

select * from staged