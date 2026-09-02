{{ config(materialized='view') }}

-- TEMP: bolt-on for the yield job mart. AI-path sales attributes, keyed
-- by so_nbr/so_line - a DJ ties to exactly one so_nbr/so_line, but a
-- sales line can span multiple shipments, so this is collapsed to one
-- row per so_nbr/so_line to guarantee no fan-out on join.

with src as (

    select *
    from {{ ref('stg_castle__sales') }}
    where lower(sales_status) = 'valid'
      and lower(line_transaction_type) like 'sales%'

)

select
    sales_order_nbr          as so_nbr,
    sales_line_nbr            as so_line,
    min(product_length)      as product_length,
    min(product_width)       as product_width,
    min(cut_length)          as cut_length,
    min(cut_width)           as cut_width

from src
group by sales_order_nbr, sales_line_nbr
