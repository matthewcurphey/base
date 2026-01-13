{{ config(materialized='view') }}

with src as (

    -- FX-aware staging (row-for-row mirror of stg_castle__dj)
    select *
    from {{ ref('int_foundation_stgcastledj_fxwpl') }}

),

/* =====================================================
   1️⃣ Roll up to discrete job grain
===================================================== */
productionorder_rows as (

    select
        'castle'                    as company,
        min(org)                    as org,
        discrete_job_no             as dj_nbr,

        max(dj_start_date)          as dj_start_date,

        -- component attributes
        max(component)              as comp_item,
        max(component_clean)        as comp_item_clean,
        max(comp_uom)               as comp_uom,

        -- expected usage fields (local FX)
        max(comp_cost)              as localfx_comp_cost,
        max(comp_qty_per_assy)      as comp_qty_per_assy,
        max(comp_req_qty)           as comp_expected_qty,

        -- FX fields (already resolved upstream)
        max(currency_code)          as currency_code,
        max(fx_rate_to_usd)         as fx_rate_to_usd,
        max(fx_effective_date)      as fx_effective_date,
        max(wpl)                    as wpl,
        max(wpl_uom)                as wpl_uom

    from src
    group by discrete_job_no
)

/* =====================================================
   2️⃣ Final projection (pure math only)
===================================================== */
select
    company,
    dj_nbr,
    dj_start_date,
    org,

    comp_item,
    comp_item_clean,
    comp_uom,

    comp_qty_per_assy,
    comp_expected_qty,

    wpl,
    wpl_uom,
    case
        when comp_uom = 'LBS' then
            coalesce(comp_expected_qty, 0)

        when comp_uom = 'MT' then
            coalesce(comp_expected_qty, 0)
            * coalesce(wpl, 0)
            * 2.2046

        when comp_uom = 'KGS' then
            coalesce(comp_expected_qty, 0)
            * 2.2046

        else
            coalesce(comp_expected_qty, 0)
            * coalesce(wpl, 0)
    end as comp_expected_lbs

from productionorder_rows
