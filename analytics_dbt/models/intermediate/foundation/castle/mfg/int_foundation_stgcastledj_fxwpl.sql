{{ config(materialized='view') }}

with src as (

    select *
    from {{ ref('stg_castle__dj') }}

),

/* =====================================================
   1) Derive currency from org (row-level)
===================================================== */
currency_derived as (

    select
        s.*,

        case
            when org in ('MXM', 'MTY', 'MCH', 'MXQ') then 'MXN'
            when org in ('ENT', 'ENA')              then 'EUR'
            else 'USD'
        end as currency_code

    from src s
),

/* =====================================================
   2) As-of FX lookup (row-level, effective dated)
===================================================== */
fx_joined as (

    select
        c.*,

        fx.fx_rate        as fx_rate_raw,
        fx.fx_date        as fx_effective_date

    from currency_derived c

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
),

/* =====================================================
   3) Join Weight-Per-Length reference (seed)
===================================================== */
wpl_joined as (

    select
        f.*,

        w.wpl_uom,
        w.wpl

    from fx_joined f
    left join {{ ref('stg_ref_wpl') }} w
      on f.component_clean = w.item
)

/* =====================================================
   4) Final projection (FX + WPL enriched staging)
===================================================== */
select
    -- ALL original stg_castle__dj columns
    date_completed,
    org,
    sales_order,
    so_line_no,
    so_line,
    so_shipment,
    discrete_job_no,

    comp_qty_per_assy,
    comp_qty_issued,
    comp_req_qty,
    comp_uom,

    component,
    component_clean,

    item,
    item_type,
    product_form,
    product_commodity,
    product_grade,
    product_item_number,
    product_shape,
    product_primary_dimension,

    product_condition_1,
    product_condition_2,
    product_condition_3,

    product_length,
    product_special_feature_1,
    product_special_feature_2,
    product_special_feature_3,
    product_surface,
    product_temper,
    product_width,

    product_item_description,

    operation_code,
    operation_sequence_number,
    resource_code,

    hrs_earned,
    dj_quantity_completed,
    primary_uom_code,

    quantity_com_weight,
    mtl_wip_value,
    dj_last_updated_by,
    applied_resource_value,
    comp_cost,
    hrs_remaining,

    job_status,
    start_quantity,
    start_quantity_weight,
    dj_start_date,

    -- FX additions
    currency_code,

    case
        when currency_code = 'USD' then 1.0
        else fx_rate_raw
    end as fx_rate_to_usd,

    fx_effective_date,

    -- WPL additions
    wpl_uom,
    wpl

from wpl_joined
