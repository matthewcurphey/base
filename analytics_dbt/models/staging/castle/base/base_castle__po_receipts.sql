{{ config(materialized='view') }}

-- 1) Pull raw CSV → Postgres data exactly as loaded
with raw as (

    select
        "PO_Receipt_Date_Value"             as po_receipt_date,
        "Vendor_Name"                       as vendor_name,
        "Vendor_Site_Code"                  as vendor_site_code,
        "Vendor_Nbr"                        as vendor_nbr,
        "Inv_Org_Code"                      as inv_org_code,
        "PO_Nbr"                            as po_nbr,
        "PO_Line_Nbr"                       as po_line_nbr,
        "Transaction_Type"                  as transaction_type,
        "Item_Nbr"                          as item_nbr,
        "Product_Item_Type"                 as product_item_type,
        "Item_Desc"                         as item_desc,
        "Product_Commodity"                 as product_commodity,
        "Product_Form"                      as product_form,
        "Product_Shape"                     as product_shape,
        "Product_Grade"                     as product_grade,
        "Product_Core_Type"                 as product_core_type,
        "POH_PO_Deliver_Pounds"             as poh_po_deliver_lbs,
        "POH_PO_Deliver_Value_(USD)"        as poh_po_deliver_usd,
        "Poh_Ordered_Qty_Pounds"            as poh_ordered_qty_lbs,
        "PO_Ordered_Value_(USD)"            as po_ordered_value_usd,
        "Supplier_Item"                     as supplier_item,
        "PO_Received_Qty"                   as po_received_qty,
        "PO_Unit_Of_Measure"                as po_uom,
        "POH_PO_Received_Pounds"            as poh_po_received_lbs,
        "POH_PO_Receive_Value(USD)"         as poh_po_receive_usd,
        "Product_Planner_Name"              as product_planner_name,
        "PO_Date"                           as po_date

    from {{ source('castle', 'castle_po_receipts') }}

),

-- 2) Clean + cast
cleaned as (

    select
        cast(po_receipt_date as date)                   as po_receipt_date,
        trim(vendor_name)                               as vendor_name,
        trim(vendor_site_code)                          as vendor_site_code,
        trim(vendor_nbr)                                as vendor_nbr,
        trim(inv_org_code)                              as inv_org_code,
        trim(po_nbr)                                    as po_nbr,
        trim(po_line_nbr)                               as po_line_nbr,
        trim(transaction_type)                          as transaction_type,
        trim(item_nbr)                                  as item_nbr,
        trim(product_item_type)                         as product_item_type,
        trim(item_desc)                                 as item_desc,
        trim(product_commodity)                         as product_commodity,
        trim(product_form)                              as product_form,
        trim(product_shape)                             as product_shape,
        trim(product_grade)                             as product_grade,
        trim(product_core_type)                         as product_core_type,
        cast(poh_po_deliver_lbs as numeric(18,4))       as poh_po_deliver_lbs,
        cast(poh_po_deliver_usd as numeric(18,4))       as poh_po_deliver_usd,
        cast(poh_ordered_qty_lbs as numeric(18,4))      as poh_ordered_qty_lbs,
        cast(po_ordered_value_usd as numeric(18,4))     as po_ordered_value_usd,
        trim(supplier_item)                             as supplier_item,
        cast(po_received_qty as numeric(18,4))          as po_received_qty,
        trim(po_uom)                                    as po_uom,
        cast(poh_po_received_lbs as numeric(18,4))      as poh_po_received_lbs,
        cast(poh_po_receive_usd as numeric(18,4))       as poh_po_receive_usd,
        trim(product_planner_name)                      as product_planner_name,
        cast(po_date as date)                           as po_date

    from raw

)

select * from cleaned
