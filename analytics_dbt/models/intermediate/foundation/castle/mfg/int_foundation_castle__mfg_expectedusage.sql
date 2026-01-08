{{ config(materialized='view') }}

with src as (

    -- Raw Castle discrete job / component expected usage
    select *
    from {{ ref('stg_castle__dj') }}

),

-- 1️⃣ Roll up to discrete job grain
productionorder_rows as (

    select
        'castle'                    as company,
        min(org)                    as org,
        discrete_job_no             as dj_nbr,

        max(dj_start_date)          as dj_start_date,

        max(component)              as comp_item,
        max(component_clean)        as comp_item_clean,
        max(comp_uom)               as comp_uom,

        -- expected usage fields
        max(comp_cost)              as localfx_comp_cost,
        max(comp_qty_per_assy)      as comp_qty_per_assy,
        max(comp_req_qty)           as comp_expected_qty

    from src
    group by discrete_job_no

),

-- 2️⃣ Derive currency from org (business rule)
currency_derived as (

    select
        *,
        case
            when org in ('MXM', 'MTY', 'MCH', 'MXQ') then 'MXN'
            when org in ('ENT', 'ENA')              then 'EUR'
            else 'USD'
        end as currency_code
    from productionorder_rows

),

-- 3️⃣ As-of FX lookup (effective dated)
fx_joined as (

    select
        p.*,

        fx.fx_rate        as fx_rate_to_usd,
        fx.fx_date        as fx_effective_date

    from currency_derived p

    -- Only look up FX for non-USD currencies
    left join lateral (
        select
            fx_date,
            fx_rate
        from raw.fxrates
        where from_currency = p.currency_code
          and to_currency   = 'USD'
          and fx_date <= p.dj_start_date
        order by fx_date desc
        limit 1
    ) fx
      on p.currency_code <> 'USD'

)

-- 4️⃣ Final projection with safe defaults
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

    currency_code,
    localfx_comp_cost,

    coalesce(fx_rate_to_usd, 1.0)           as fx_rate_to_usd,
    fx_effective_date,

    -- expected cost in USD
    localfx_comp_cost
        * coalesce(fx_rate_to_usd, 1.0)      as comp_cost_usd,

    localfx_comp_cost
        * coalesce(fx_rate_to_usd, 1.0)
        * comp_expected_qty                as comp_expected_usd

from fx_joined
