{{ config(materialized='view') }}

with base as (
    select * from {{ ref('base_castle_oracle__inventory') }}
),

staged as (

    select

        org::text                                           as org,
        org_name::text                                      as org_name,
        item::text                                          as item,
        tms1::text                                          as tms1,
        tms2::text                                          as tms2,
        
        form::text                                          as form,
        alloy::text                                         as alloy,
        temper::text                                        as temper,
        spec::text                                          as spec,
        thk::numeric(18,6)                                  as thk,
        item_desc::text                                     as item_desc,

        lot_nbr::text                                       as lot_nbr,
        heat_nbr::text                                      as heat_nbr,
        lot_length::numeric(18,6)                           as lot_length,
        lot_width::numeric(18,6)                            as lot_width,

        sub_inv::text                                       as sub_inv,
        locator::text                                       as locator,

        on_hand_pcs::numeric(18,6)                              as on_hand_pcs,
        on_hand_weight::numeric(18,6)                           as on_hand_weight_kg,
        round((on_hand_weight::numeric) * 2.20462262184878, 6) as on_hand_lbs,
        on_hand_usd::numeric(18,4)                              as on_hand_usd,

        weight_per_ft_or_mt::numeric(18,6)                  as weight_per_ft_or_mt,

        mill::text                                          as mill,
        amc_age::numeric(18,2)                              as amc_age

    from base

)

select * from staged
