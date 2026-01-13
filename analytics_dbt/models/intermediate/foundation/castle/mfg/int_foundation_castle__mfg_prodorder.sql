{{ config(materialized='view') }}

with src as (

    select *
    from {{ ref('int_foundation_stgcastledj_fxwpl') }}

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
        min(dj_last_updated_by)         as dj_last_updated_by,

        /* =======================
           Component + FX (upstream)
        ======================= */
        max(component)              as comp_item,
        max(component_clean)        as comp_item_clean,
        max(comp_uom)               as comp_uom,
        max(comp_cost)              as localfx_comp_cost,

        max(currency_code)          as currency_code,
        max(fx_rate_to_usd)         as fx_rate_to_usd,
        max(fx_effective_date)      as fx_effective_date,
        max(wpl)                    as wpl,
        max(wpl_uom)                as wpl_uom
        

    from src
    group by discrete_job_no
)

/* =====================================================
   2) Final projection (pure math only)
===================================================== */
select
    company,
    dj_nbr,
    complete_date,
    dj_start_date,
    org,

    so_nbr,
    so_line,
    so_shipment,

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

    complete_qty,
    start_qty,
    job_uom,
    start_qty_weight,

    job_status,
    dj_last_updated_by,

    comp_item,
    comp_item_clean,
    comp_uom,

    currency_code,
    localfx_comp_cost,
    fx_rate_to_usd,
    fx_effective_date,

    localfx_comp_cost
        * fx_rate_to_usd            as comp_cost_usd,
    wpl,
    wpl_uom,
    complete_qty_weight             as comp_complete_lbs


from productionorder_rows
