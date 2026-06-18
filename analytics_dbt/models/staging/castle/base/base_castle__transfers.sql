{{ config(materialized='view') }}

-- 1) Pull raw CSV → Postgres data exactly as loaded
with raw as (

    select
        "Inv_Org_Name"                          as inv_org_name,
        "Product_Primary_Item_Nbr"              as product_primary_item_nbr,
        "Item_Nbr"                              as item_nbr,
        "Product_Form"                          as product_form,
        "Product_Grade"                         as product_grade,
        "Lines"                                 as lines,
        "Weight"                                as weight_lbs,
        "Ordered_Quantity"                      as ordered_qty,
        "Material_Revenue"                      as material_revenue,
        "Material_Revenue_(Local_Currency)"     as material_revenue_local,
        "Lines_Shipped"                         as lines_shipped,
        "Ordered_Unit_of_Measure"               as ordered_uom,
        "Sales_Status"                          as sales_status,
        "Sales_Type"                            as sales_type,
        "Shipment_Nbr"                          as shipment_nbr,
        "Sales_Line_Nbr"                        as sales_line_nbr,
        "Order_Number"                          as order_number,
        "Shipping_Inventory_Org_Name"           as shipping_inv_org_name,
        "PO_Date"                               as po_date,
        "PO_Due_Date"                           as po_due_date,
        "Last_Receipt_Date"                     as last_receipt_date

    from {{ source('castle', 'castle_transfers') }}

),

-- 2) Clean + cast
cleaned as (

    select
        trim(inv_org_name)                              as inv_org_name,
        trim(product_primary_item_nbr)                  as product_primary_item_nbr,
        trim(item_nbr)                                  as item_nbr,
        trim(product_form)                              as product_form,
        trim(product_grade)                             as product_grade,
        cast(lines as numeric(18,4))                    as lines,
        cast(weight_lbs as numeric(18,4))               as weight_lbs,
        cast(ordered_qty as numeric(18,4))              as ordered_qty,
        cast(material_revenue as numeric(18,4))         as material_revenue,
        cast(material_revenue_local as numeric(18,4))   as material_revenue_local,
        cast(lines_shipped as numeric(18,4))            as lines_shipped,
        trim(ordered_uom)                               as ordered_uom,
        trim(sales_status)                              as sales_status,
        trim(sales_type)                                as sales_type,
        trim(shipment_nbr)                              as shipment_nbr,
        trim(sales_line_nbr)                            as sales_line_nbr,
        trim(order_number)                              as order_number,
        trim(shipping_inv_org_name)                     as shipping_inv_org_name,
        cast(po_date as date)                           as po_date,
        cast(po_due_date as date)                       as po_due_date,
        cast(last_receipt_date as date)                 as last_receipt_date

    from raw

)

select * from cleaned
