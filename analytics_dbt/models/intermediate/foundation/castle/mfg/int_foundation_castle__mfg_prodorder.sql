{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_castle__dj') }}
),

-- 1) Roll up to discrete job level
productionorder_rows as (
    select
        'castle'                  as company,
        discrete_job_no           as dj_nbr,

        /* =======================
           IDENTIFIERS & KEYS
        ======================= */
        max(date_completed)       as complete_date,
        min(org)                  as org,

        -- SO fields (first non-null)
        min(sales_order)          as so_nbr,
        min(so_line)              as so_line,
        min(so_shipment)          as so_shipment,

        /* =======================
           PRODUCT ATTRIBUTES
           (usually 1:1 at job level)
        ======================= */
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

        /* =======================
           QUANTITIES
        ======================= */

        max(dj_quantity_completed)      as complete_qty,
        max(start_quantity)             as start_qty,
        min(primary_uom_code)           as job_uom,
        max(start_quantity_weight)      as start_qty_weight,
        max(quantity_com_weight)        as complete_qty_weight,


        /* =======================
           JOB METADATA
        ======================= */
        min(job_status)                 as job_status,
        min(dj_start_date)              as dj_start_date,
        min(dj_last_updated_by)         as dj_last_updated_by
        

    from src
    group by
        discrete_job_no
)

select *
from productionorder_rows
