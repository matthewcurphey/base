{{ config(materialized='view') }}

-- 1) Pull raw CSV → Postgres data exactly as loaded
with raw as (

    select
        "Date_Completed"                      as date_completed,
        "Org"                                 as org,
        "Sales_Order"                         as sales_order,
        "So_Line_No"                          as so_line_no,
        "Discrete_Job_No"                     as discrete_job_no,
        "Comp_Qty_Per_Assy"                   as comp_qty_per_assy,
        "Comp_Qtyp_Issued"                    as comp_qty_issued,
        "Comp_Req_Qty"                        as comp_req_qty,
        "Comp_Uom"                            as comp_uom,
        "Component"                           as component,
        "Item"                                as item,
        "Item_Type"                           as item_type,
        "Product_Form"                        as product_form,
        "Product_Commodity"                   as product_commodity,
        "Product_Grade"                       as product_grade,
        "Product_Item_Number"                 as product_item_number,
        "Product_Shape"                       as product_shape,
        "Product_Primmary__Dimention"         as product_primary_dimension,
        "Product_Condition_1"                 as product_condition_1,
        "Product_Condition_2"                 as product_condition_2,
        "Product_Condition_3"                 as product_condition_3,
        "Product_Length"                      as product_length,
        "Product_Special_Feature_1"           as product_special_feature_1,
        "Product_special_Feature_2"           as product_special_feature_2,
        "Product_Special_Feature_3"           as product_special_feature_3,
        "Product__Surface"                    as product_surface,
        "Product_Temper"                      as product_temper,
        "Product__Width"                      as product_width,
        "Product_Item_Description"            as product_item_description,
        "Operation_Code"                      as operation_code,
        "Resource_Code"                       as resource_code,
        "Hrs_Earned"                          as hrs_earned,
        "DJ_Quantity_Completed"               as dj_quantity_completed,
        "Primary_Uom_Code"                    as primary_uom_code,
        "Quantity_Com_Weight"                 as quantity_com_weight,
        "Mtl_Wip_Value"                       as mtl_wip_value,
        "DJ_Last_Updated_BY"                  as dj_last_updated_by,
        "Applied_Resource_Value"              as applied_resource_value,
        "Comp_Cost"                           as comp_cost,
        "Hrs_Remaining"                       as hrs_remaining,
        "Job_Status"                          as job_status,
        "Start_Quantity"                      as start_quantity,
        "Start_Quantity_Weight"               as start_quantity_weight,
        "DJ_Start_Date"                       as dj_start_date

    from {{ source('castle', 'castle_dj') }}
),

-- 2) Trim + normalize + standard CAST types
cleaned as (

    select
        cast(date_completed as date)                 as date_completed,
        trim(org)                                    as org,
        trim(sales_order)                            as sales_order,
        trim(so_line_no)                             as so_line_no,
        trim(discrete_job_no)                        as discrete_job_no,

        cast(comp_qty_per_assy as numeric(18,6))     as comp_qty_per_assy,
        cast(comp_qty_issued as numeric(18,6))        as comp_qty_issued,
        cast(comp_req_qty as numeric(18,6))           as comp_req_qty,
        trim(comp_uom)                                as comp_uom,

        trim(component)                               as component,
        trim(item)                                    as item,
        trim(item_type)                               as item_type,
        trim(product_form)                            as product_form,
        trim(product_commodity)                       as product_commodity,
        trim(product_grade)                           as product_grade,
        trim(product_item_number)                     as product_item_number,
        trim(product_shape)                           as product_shape,
        trim(product_primary_dimension)               as product_primary_dimension,

        trim(product_condition_1)                     as product_condition_1,
        trim(product_condition_2)                     as product_condition_2,
        trim(product_condition_3)                     as product_condition_3,

        cast(product_length as numeric(18,6))         as product_length,
        trim(product_special_feature_1)               as product_special_feature_1,
        trim(product_special_feature_2)               as product_special_feature_2,
        trim(product_special_feature_3)               as product_special_feature_3,
        trim(product_surface)                         as product_surface,
        trim(product_temper)                          as product_temper,
        cast(product_width as numeric(18,6))          as product_width,

        trim(product_item_description)                as product_item_description,

        trim(operation_code)                          as operation_code,
        trim(resource_code)                           as resource_code,

        cast(hrs_earned as numeric(18,6))             as hrs_earned,
        cast(dj_quantity_completed as numeric(18,6))   as dj_quantity_completed,
        trim(primary_uom_code)                        as primary_uom_code,

        cast(quantity_com_weight as numeric(18,6))    as quantity_com_weight,
        cast(mtl_wip_value as numeric(18,4))          as mtl_wip_value,
        trim(dj_last_updated_by)                      as dj_last_updated_by,
        cast(applied_resource_value as numeric(18,4)) as applied_resource_value,
        cast(comp_cost as numeric(18,4))              as comp_cost,
        cast(hrs_remaining as numeric(18,6))          as hrs_remaining,

        trim(job_status)                              as job_status,

        cast(start_quantity as numeric(18,6))         as start_quantity,
        cast(start_quantity_weight as numeric(18,6))  as start_quantity_weight,

        cast(dj_start_date as date)                   as dj_start_date

    from raw
)

select * from cleaned