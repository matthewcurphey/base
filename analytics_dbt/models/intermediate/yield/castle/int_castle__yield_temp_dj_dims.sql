{{ config(materialized='view') }}

-- TEMP: bolt-on for the yield job mart. Pulls DJ-level item_type and
-- product dimensions, which exist on the DJ source but get dropped
-- further down the existing yield chain (int_castle__yield_01 onward).
-- Collapsed to one row per dj_nbr (same min()-per-job convention already
-- used in int_foundation_castle__mfg_prodorder) so it can't fan out
-- whatever it's joined onto.

with src as (

    select *
    from {{ ref('int_foundation_stgcastledj_fxwpl') }}

)

select
    discrete_job_no              as dj_nbr,
    min(sales_order)             as so_nbr,
    min(so_line)                 as so_line,
    min(item_type)               as item_type,
    min(product_item_number)     as product_item_number,
    min(product_length)          as product_length,
    min(product_width)           as product_width,

    -- Component item (raw material) - same aggregate choice (max) as
    -- comp_item in int_foundation_castle__mfg_prodorder, for consistency.
    max(component_clean)         as comp_item_clean,

    -- Completed quantity + its UOM - same aggregate choices (max/min) as
    -- complete_qty/job_uom in int_foundation_castle__mfg_prodorder. Almost
    -- always PCS but not guaranteed, so carry the UOM alongside rather
    -- than assuming it.
    max(dj_quantity_completed)   as complete_qty,
    min(primary_uom_code)        as job_uom

from src
group by discrete_job_no
