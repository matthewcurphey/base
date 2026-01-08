{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_castle__dj') }}
),

/* =====================================================
   1) Roll up to discrete job level
===================================================== */
productionorder_rows as (
    select
        'castle'                  as company,
        discrete_job_no           as dj_nbr,

        max(date_completed)       as complete_date,
        min(org)                  as org,

        min(sales_order)          as so_nbr,
        min(so_line)              as so_line,
        min(so_shipment)          as so_shipment,

        min(product_form)                       as product_form,
        min(product_commodity)                  as product_commodity,
        min(product_grade)                      as product_grade,
        min(product_item_number)                as product_item_number,
        min(product_shape)                      as product_shape,
        min(product_primary_dimension)          as product_primary_dimension,
        min(product_condition_1)                as product_condition_1,
        min(product_condition_2)                as product_condition_2,
        min(product_condition_3)                as product_condition_3,
        min(product_length)                     as product_length,
        min(product_special_feature_1)          as product_special_feature_1,
        min(product_special_feature_2)          as product_special_feature_2,
        min(product_special_feature_3)          as product_special_feature_3,
        min(product_surface)                    as product_surface,
        min(product_temper)                     as product_temper,
        min(product_width)                      as product_width,
        min(product_item_description)           as product_item_description,

        max(dj_quantity_completed)      as complete_qty,
        max(start_quantity)             as start_qty,
        min(primary_uom_code)           as job_uom,
        max(start_quantity_weight)      as start_qty_weight,
        max(quantity_com_weight)        as complete_qty_weight,

        min(job_status)                 as job_status,
        min(dj_start_date)              as dj_start_date,
        min(dj_last_updated_by)         as dj_last_updated_by

    from src
    group by discrete_job_no
),

/* =====================================================
   2) Component rollup (job grain)
===================================================== */
component_rows as (
    select
        discrete_job_no           as dj_nbr,

        max(component)            as comp_item,
        max(component_clean)      as comp_item_clean,
        max(comp_uom)             as comp_uom,
        max(comp_cost)            as localfx_comp_cost,

        case
            when min(org) in ('MXM', 'MTY', 'MCH', 'MXQ') then 'MXN'
            when min(org) in ('ENT', 'ENA')              then 'EUR'
            else 'USD'
        end                        as currency_code,

        min(dj_start_date)        as dj_start_date

    from src
    group by discrete_job_no
),

/* =====================================================
   3) FX lookup (EXACT parity with existing logic)
===================================================== */
fx_joined as (

    select
        c.*,

        fx.fx_rate        as fx_rate_to_usd,
        fx.fx_date        as fx_effective_date

    from component_rows c

    left join lateral (
        select
            fx_date,
            fx_rate
        from raw.fxrates
        where from_currency = c.currency_code
          and to_currency   = 'USD'
          and fx_date <= c.dj_start_date
        order by fx_date desc
        limit 1
    ) fx
      on c.currency_code <> 'USD'
)

/* =====================================================
   4) Final projection
===================================================== */
select
    p.*,

    f.comp_item,
    f.comp_item_clean,
    f.comp_uom,

    f.currency_code,
    f.localfx_comp_cost,

    coalesce(f.fx_rate_to_usd, 1.0)           as fx_rate_to_usd,
    f.fx_effective_date,

    f.localfx_comp_cost
        * coalesce(f.fx_rate_to_usd, 1.0)         as comp_cost_usd

from productionorder_rows p
left join fx_joined f
  on p.dj_nbr = f.dj_nbr
