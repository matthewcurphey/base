{{ config(materialized='view') }}

-- 1) Pull raw CSV → Postgres data exactly as loaded
with raw as (

    select
        "Vendor_Name"                   as vendor_name,
        "Vendor_Site_Code"              as vendor_site_code,
        "Vendor_Nbr"                    as vendor_nbr,
        "Inv_Org_Code"                  as inv_org_code,
        "PO_Nbr"                        as po_nbr,
        "PO_Line_Nbr"                   as po_line_nbr,
        "Supplier_Item"                 as supplier_item,
        "Item_Nbr"                      as item_nbr,
        "Item_Desc"                     as item_desc,
        "Product_Grade"                 as product_grade,
        "Product_Shape"                 as product_shape,
        "PO_Open_LBS"                   as po_open_lbs,
        "Buyer"                         as buyer,
        "Product_Item_Type"             as product_item_type,
        "Product_Core_Type"             as product_core_type,
        "Product_Lead_Time"             as product_lead_time,
        "Open_US$"                      as open_usd,
        "PO_Due_Date"                   as po_due_date,
        "PO_Status_Code"                as po_status_code,
        "PO_Orig_Due_Date_Value"        as po_orig_due_date,
        "PO_US$"                        as po_usd,
        "PO_Ordered_Lbs"                as po_ordered_lbs,
        "PO_Ordered_Units"              as po_ordered_units,
        "PO_Open_Qty"                   as po_open_qty,
        "PO_UOM"                        as po_uom,
        "Description"                   as description,
        "Product_Form"                  as product_form,
        "Product_Primary_Item_Nbr"      as product_primary_item_nbr,
        "Product_Commodity"             as product_commodity,
        "PO_Ordered_Qty"                as po_ordered_qty,
        "PO_Received_Lbs"               as po_received_lbs,
        "Acceptance_type"               as acceptance_type,
        "Action"                        as action,
        "Action_date"                   as action_date,
        "PO_Received_Units"             as po_received_units,
        "PO_Date"                       as po_date

    from {{ source('castle', 'castle_po_open') }}

),

-- 2) Clean + cast
cleaned as (

    select
        trim(vendor_name)                               as vendor_name,
        trim(vendor_site_code)                          as vendor_site_code,
        trim(vendor_nbr)                                as vendor_nbr,
        trim(inv_org_code)                              as inv_org_code,
        trim(po_nbr)                                    as po_nbr,
        trim(po_line_nbr)                               as po_line_nbr,
        trim(supplier_item)                             as supplier_item,
        trim(item_nbr)                                  as item_nbr,
        trim(item_desc)                                 as item_desc,
        trim(product_grade)                             as product_grade,
        trim(product_shape)                             as product_shape,
        cast(po_open_lbs as numeric(18,4))              as po_open_lbs,
        trim(buyer)                                     as buyer,
        trim(product_item_type)                         as product_item_type,
        trim(product_core_type)                         as product_core_type,
        cast(product_lead_time as numeric(18,2))        as product_lead_time,
        cast(open_usd as numeric(18,4))                 as open_usd,
        cast(po_due_date as date)                       as po_due_date,
        trim(po_status_code)                            as po_status_code,
        cast(po_orig_due_date as date)                  as po_orig_due_date,
        cast(po_usd as numeric(18,4))                   as po_usd,
        cast(po_ordered_lbs as numeric(18,4))           as po_ordered_lbs,
        cast(po_ordered_units as numeric(18,4))         as po_ordered_units,
        cast(po_open_qty as numeric(18,4))              as po_open_qty,
        trim(po_uom)                                    as po_uom,
        trim(description)                               as description,
        trim(product_form)                              as product_form,
        trim(product_primary_item_nbr)                  as product_primary_item_nbr,
        trim(product_commodity)                         as product_commodity,
        cast(po_ordered_qty as numeric(18,4))           as po_ordered_qty,
        cast(po_received_lbs as numeric(18,4))          as po_received_lbs,
        trim(acceptance_type)                           as acceptance_type,
        trim(action)                                    as action,
        case when trim(action_date) = '0-00-00' then null else cast(action_date as date) end as action_date,
        cast(po_received_units as numeric(18,4))        as po_received_units,
        cast(po_date as date)                           as po_date

    from raw

)

select * from cleaned
