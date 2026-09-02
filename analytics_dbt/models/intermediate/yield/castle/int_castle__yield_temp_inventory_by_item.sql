{{ config(materialized='view') }}

-- TEMP: bolt-on for the yield job mart. Component-item dimensions for the
-- FG path, keyed by item_nbr. int_foundation_castle__inv_inventory is
-- lot/on-hand grain (many rows per item - different lots, locations), not
-- an item master, so this collapses to one row per item first - confirmed
-- product_length/product_width never actually vary within an item_nbr.

with src as (

    select *
    from {{ ref('int_foundation_castle__inv_inventory') }}

)

select
    item_nbr,
    min(product_length)      as product_length,
    min(product_width)       as product_width

from src
group by item_nbr
