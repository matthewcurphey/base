{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_castle__dj') }}
),

staged as (

    select
        date_completed::date                       as date_completed,
        org::text                                  as org,
        sales_order::text                           as sales_order,
        so_line_no::text                            as so_line_no,
        discrete_job_no::text                       as discrete_job_no,

        comp_qty_per_assy::numeric(18,6)           as comp_qty_per_assy,
        comp_qty_issued::numeric(18,6)             as comp_qty_issued,
        comp_req_qty::numeric(18,6)                as comp_req_qty,
        comp_uom::text                             as comp_uom,

        component::text                            as component,
        item::text                                 as item,
        item_type::text                            as item_type,
        product_form::text                         as product_form,
        product_commodity::text                    as product_commodity,
        product_grade::text                        as product_grade,
        product_item_number::text                  as product_item_number,
        product_shape::text                        as product_shape,
        product_primary_dimension::text            as product_primary_dimension,

        product_condition_1::text                  as product_condition_1,
        product_condition_2::text                  as product_condition_2,
        product_condition_3::text                  as product_condition_3,

        product_length::numeric(18,6)              as product_length,
        product_special_feature_1::text            as product_special_feature_1,
        product_special_feature_2::text            as product_special_feature_2,
        product_special_feature_3::text            as product_special_feature_3,
        product_surface::text                      as product_surface,
        product_temper::text                       as product_temper,
        product_width::numeric(18,6)               as product_width,

        product_item_description::text             as product_item_description,

        operation_code::text                       as operation_code,
        resource_code::text                        as resource_code,

        hrs_earned::numeric(18,6)                  as hrs_earned,
        dj_quantity_completed::numeric(18,6)        as dj_quantity_completed,
        primary_uom_code::text                     as primary_uom_code,

        quantity_com_weight::numeric(18,6)         as quantity_com_weight,
        mtl_wip_value::numeric(18,4)               as mtl_wip_value,
        dj_last_updated_by::text                   as dj_last_updated_by,
        applied_resource_value::numeric(18,4)      as applied_resource_value,
        comp_cost::numeric(18,4)                   as comp_cost,
        hrs_remaining::numeric(18,6)               as hrs_remaining,

        job_status::text                           as job_status,

        start_quantity::numeric(18,6)              as start_quantity,
        start_quantity_weight::numeric(18,6)       as start_quantity_weight,

        dj_start_date::date                        as dj_start_date

    from base
)

select * from staged