{{ config(materialized='view') }}

-- 1) Pull raw CSV → Postgres data exactly as loaded
with raw as (

    select
        "Inv_Org_Code"                    as inv_org_code,
        "Product_Primary_Item_Nbr"        as product_primary_item_nbr,
        "Item_Desc"                       as item_desc,
        "Product_Commodity"               as product_commodity,
        "Product_Shape"                   as product_shape,
        "Product_Form"                    as product_form,
        "Product_Grade"                   as product_grade,
        "ProductTemper"                   as product_temper,
        "Product_AMC_Container_Spec"      as product_amc_container_spec,
        "Product_Primary_Dimension"       as product_primary_dimension,
        "Product_Length"                  as product_length,
        "Product_Width"                   as product_width,
        "Product_Item_Type"               as product_item_type,
        "Sub_Inv_Code"                    as sub_inv_code,
        "On_Hand_Units"                   as on_hand_units,
        "Product_Stocking_UOM"            as product_stocking_uom,
        "On_Hand_US$"                     as on_hand_usd,
        "Lot_Nbr"                         as lot_nbr,
        "Lot_Length"                      as lot_length,
        "Lot_Width"                       as lot_width,
        "Heat_Nbr"                        as heat_nbr,
        "On_Hand_Lbs"                     as on_hand_lbs,
        "On_Hand_US$.1"                   as on_hand_usd_2,
        "Lot_Aging_in_Days"               as lot_aging_in_days,
        "Lot_Creation_Date"               as lot_creation_date,
        "Company_Origin_Date"             as company_origin_date,
        "Company_Origin_Lot"              as company_origin_lot,
        "On_hand_Material_(USD)"          as on_hand_material_usd,
        "Warehouse_Locator"               as warehouse_locator,
        "inv_date"                        as inv_date,
        "Item_Nbr"                        as item_nbr

    from {{ source('castle', 'castle_inventory') }}
),

-- 2) Clean + cast
cleaned as (

    select
        trim(inv_org_code)                         as inv_org_code,
        trim(product_primary_item_nbr)             as product_primary_item_nbr,
        trim(item_desc)                            as item_desc,
        trim(product_commodity)                    as product_commodity,
        trim(product_shape)                        as product_shape,
        trim(product_form)                         as product_form,
        trim(product_grade)                        as product_grade,
        trim(product_temper)                       as product_temper,
        trim(product_amc_container_spec)           as product_amc_container_spec,
        cast(product_primary_dimension as numeric(18,6))            as product_primary_dimension,

        cast(product_length as numeric(18,6))      as product_length,
        cast(product_width as numeric(18,6))       as product_width,

        trim(product_item_type)                    as product_item_type,
        trim(sub_inv_code)                         as sub_inv_code,

        cast(on_hand_units as numeric(18,6))       as on_hand_units,
        trim(product_stocking_uom)                 as product_stocking_uom,

        cast(on_hand_usd as numeric(18,4))         as on_hand_usd,
        trim(lot_nbr)                              as lot_nbr,

        cast(lot_length as numeric(18,6))          as lot_length,
        cast(lot_width as numeric(18,6))           as lot_width,

        trim(heat_nbr)                             as heat_nbr,

        cast(on_hand_lbs as numeric(18,6))         as on_hand_lbs,
        cast(on_hand_usd_2 as numeric(18,4))       as on_hand_usd_2,

        cast(lot_aging_in_days as numeric(18,2))   as lot_aging_in_days,

        cast(lot_creation_date as date)            as lot_creation_date,
        cast(company_origin_date as date)           as company_origin_date,

        trim(company_origin_lot)                   as company_origin_lot,
        cast(on_hand_material_usd as numeric(18,4)) as on_hand_material_usd,

        trim(warehouse_locator)                    as warehouse_locator,
        cast(inv_date as date)                     as inv_date,

        trim(item_nbr)                             as item_nbr

    from raw
)

select * from cleaned