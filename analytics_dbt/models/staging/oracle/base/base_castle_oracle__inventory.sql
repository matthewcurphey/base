{{ config(materialized='view') }}

with raw as (

    select
        "TMS1"                  as tms1,
        "TMS2"                  as tms2,
        "ALLOY"                 as alloy,
        "TEMPER"                as temper,
        "SPEC"                  as spec,
        "PriWHSE"               as org_name,
        "OnHandPCS"             as on_hand_pcs,
        "OnHandWeight"          as on_hand_weight,
        "OnHandValue"           as on_hand_usd,
        "FORM"                  as form,
        "HEATPO"                as heat_nbr,
        "WeightPerFTorMT"       as weight_per_ft_or_mt,
        "SubInv"                as sub_inv,
        "Locator"               as locator,
        "THK"                   as thk,
        "Length"                as lot_length,
        "Width"                 as lot_width,
        "ITEM"                  as item,
        "AMC_Age"               as amc_age,
        "LOTNo"                 as lot_nbr,
        "DESC"                  as item_desc,
        "MILL"                  as mill,
        "org"                   as org

    from {{ source('oracle', 'castle_oracle_inventory') }}

),

cleaned as (

    select
        trim(tms1)              as tms1,
        trim(tms2)              as tms2,
        trim(alloy)             as alloy,
        trim(temper)            as temper,
        trim(spec)              as spec,
        trim(org_name)          as org_name,
        trim(on_hand_pcs)       as on_hand_pcs,
        trim(on_hand_weight)    as on_hand_weight,
        trim(on_hand_usd)       as on_hand_usd,
        trim(form)              as form,
        trim(heat_nbr)          as heat_nbr,
        trim(weight_per_ft_or_mt) as weight_per_ft_or_mt,
        trim(sub_inv)           as sub_inv,
        trim(locator)           as locator,
        trim(thk)               as thk,
        trim(lot_length)        as lot_length,
        trim(lot_width)         as lot_width,
        trim(item)              as item,
        trim(amc_age)           as amc_age,
        trim(lot_nbr)           as lot_nbr,
        trim(item_desc)         as item_desc,
        trim(mill)              as mill,
        trim(org)               as org

    from raw

)

select * from cleaned
