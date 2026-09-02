{{ config(materialized='view') }}

-- TEMP: bolt-on for the yield job mart. FG-path sales attributes - FG
-- items carry the same product length/width regardless of which order
-- they're on, so this is keyed by item number rather than so_nbr/so_line.
-- No cut_length/cut_width here: cut size is order-specific, and an
-- item-level lookup has no single order to pull it from.

with src as (

    select *
    from {{ ref('stg_castle__sales') }}
    where lower(sales_status) = 'valid'
      and lower(line_transaction_type) like 'sales%'

)

select
    product_item_nbr         as item_nbr,
    min(product_length)      as product_length,
    min(product_width)       as product_width

from src
group by product_item_nbr
