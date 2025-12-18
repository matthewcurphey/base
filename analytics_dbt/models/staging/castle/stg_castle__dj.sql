{{ config(materialized='view') }}

WITH base AS (
    SELECT *
    FROM {{ ref('base_castle__dj') }}
),

-- Clean up and split so_line_no
staged AS (

    SELECT
        date_completed::date                       AS date_completed,
        org::text                                  AS org,
        sales_order::text                          AS sales_order,

        -- Raw field preserved
        so_line_no::text                           AS so_line_no,

        -- Normalize ".", "", null → NULL
        CASE 
            WHEN so_line_no IS NULL OR so_line_no IN ('', '.') THEN NULL
            ELSE split_part(so_line_no, '.', 1)
        END::text                                   AS so_line,

        CASE 
            WHEN so_line_no IS NULL OR so_line_no IN ('', '.') THEN NULL
            ELSE split_part(so_line_no, '.', 2)
        END::text                                   AS so_shipment,

        discrete_job_no::text                      AS discrete_job_no,

        comp_qty_per_assy::numeric(18,6)           AS comp_qty_per_assy,
        comp_qty_issued::numeric(18,6)             AS comp_qty_issued,
        comp_req_qty::numeric(18,6)                AS comp_req_qty,
        comp_uom::text                             AS comp_uom,

        component::text                            AS component,
        item::text                                 AS item,
        item_type::text                            AS item_type,
        product_form::text                         AS product_form,
        product_commodity::text                    AS product_commodity,
        product_grade::text                        AS product_grade,
        product_item_number::text                  AS product_item_number,
        product_shape::text                        AS product_shape,
        product_primary_dimension::numeric(18,6)              AS product_primary_dimension,

        product_condition_1::text                  AS product_condition_1,
        product_condition_2::text                  AS product_condition_2,
        product_condition_3::text                  AS product_condition_3,

        product_length::numeric(18,6)              AS product_length,
        product_special_feature_1::text            AS product_special_feature_1,
        product_special_feature_2::text            AS product_special_feature_2,
        product_special_feature_3::text            AS product_special_feature_3,
        product_surface::text                      AS product_surface,
        product_temper::text                       AS product_temper,
        product_width::numeric(18,6)               AS product_width,

        product_item_description::text             AS product_item_description,

        operation_code::text                       AS operation_code,
        operation_sequence_number::int            AS operation_sequence_number,
        resource_code::text                        AS resource_code,

        hrs_earned::numeric(18,6)                  AS hrs_earned,
        dj_quantity_completed::numeric(18,6)       AS dj_quantity_completed,
        primary_uom_code::text                     AS primary_uom_code,

        quantity_com_weight::numeric(18,6)         AS quantity_com_weight,
        mtl_wip_value::numeric(18,4)               AS mtl_wip_value,
        dj_last_updated_by::text                   AS dj_last_updated_by,
        applied_resource_value::numeric(18,4)      AS applied_resource_value,
        comp_cost::numeric(18,4)                   AS comp_cost,
        hrs_remaining::numeric(18,6)               AS hrs_remaining,

        job_status::text                           AS job_status,

        start_quantity::numeric(18,6)              AS start_quantity,
        start_quantity_weight::numeric(18,6)       AS start_quantity_weight,

        dj_start_date::date                        AS dj_start_date

    FROM base
)

SELECT * FROM staged