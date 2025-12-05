{{ config(materialized='view') }}

-- 1) Pull raw CSV → Postgres data exactly as loaded
with raw as (

    select
        "Branch_Name"                     as branch_name,
        "Inv_Org_Code"                    as inv_org_code,
        "Order_Date"                      as order_date,
        "Invoice_Date"                    as invoice_date,
        "Actual_Ship_Date"                as actual_ship_date,
        "Quote_Date"                      as quote_date,
        "Request_Date"                    as request_date,
        "Promise_Date"                    as promise_date,
        "Sales_Status"                    as sales_status,
        "Sales_Type"                      as sales_type,
        "Sales_Order_Nbr"                 as sales_order_nbr,
        "Sales_Line_Nbr"                  as sales_line_nbr,
        "Shipment_Nbr"                    as shipment_nbr,
        "Line_Transaction_Type"           as line_transaction_type,
        "Cut_UOM"                         as cut_uom,
        "Cut_Shape"                       as cut_shape,
        "Cut_Width"                       as cut_width,
        "Cut_Length"                      as cut_length,
        "Ordered_Qty"                     as ordered_qty,
        "Ordered_UOM"                     as ordered_uom,
        "Ordered_Pcs"                     as ordered_pcs,
        "Freight_Cost"                    as freight_cost,
        "Freight_Revenue_(US$)"           as freight_revenue_usd,
        "Material_Revenue"                as material_revenue,
        "Proc_Rev_(US$)"                  as proc_rev_usd,
        "Material_Cost(AAC)"              as material_cost_aac,
        "Material_Overhead_Cost"          as material_overhead_cost,
        "Outside_Processing_Cost"         as outside_processing_cost,
        "Resource_Cost_(US$)"             as resource_cost_usd,
        "Total_Gross_Profit_(US$)"        as total_gross_profit_usd,
        "List_Price_per_Lbs_(Gross)"      as list_price_per_lbs_gross,
        "Price_per_Lbs_(Gross)"           as price_per_lbs_gross,
        "Total_Sales_(US$)"               as total_sales_usd,
        "Weight_(lbs)"                    as weight_lbs,
        "Gross_Weight_(lbs)"              as gross_weight_lbs,
        "Invoiced_Lbs"                    as invoiced_lbs,
        "Invoiced_Pcs"                    as invoiced_pcs,
        "Invoiced_Qty"                    as invoiced_qty,
        "UOM"                             as uom,
        "Ordered_Lbs"                     as ordered_lbs,
        "Matl_GP_(US$)"                   as matl_gp_usd,
        "TGP_pct"                         as tgp_pct,
        "MGP_pct"                         as mgp_pct,
        "Absorption_Cost_(US$)"           as absorption_cost_usd,
        "Product_Item_Nbr"                as product_item_nbr,
        "Product_Item_Type"               as product_item_type,
        "Product_Primary_Item_Nbr"        as product_primary_item_nbr,
        "Product_Form"                    as product_form,
        "Product_Grade"                   as product_grade,
        "Product_Customer"                as product_customer,
        "Product_Shape"                   as product_shape,
        "Product_Stocking_UOM"            as product_stocking_uom,
        "Product_Primary_Dimension"       as product_primary_dimension,
        "Product_Temper"                  as product_temper,
        "Product_Length"                  as product_length,
        "Product_Width"                   as product_width,
        "Product_Commodity"               as product_commodity,
        "Product_Item_Description"        as product_item_description,
        "Product_Source_Type"             as product_source_type,
        "Ship_To_Customer_Name"           as ship_to_customer_name,
        "Ship_To_Customer_Nbr"            as ship_to_customer_nbr,
        "Sold_To_Customer_Name"           as sold_to_customer_name,
        "Sold_To_Customer_Nbr"            as sold_to_customer_nbr

    from {{ source('castle', 'castle_sales') }}
),

-- 2) Trim + normalize + standard CAST types
cleaned as (

    select
        /* Identifiers & Strings */
        trim(branch_name)                     as branch_name,
        trim(inv_org_code)                    as inv_org_code,
        trim(sales_status)                    as sales_status,
        trim(sales_type)                      as sales_type,
        trim(sales_order_nbr)                 as sales_order_nbr,
        trim(sales_line_nbr)                  as sales_line_nbr,
        trim(shipment_nbr)                    as shipment_nbr,
        trim(line_transaction_type)           as line_transaction_type,

        trim(cut_uom)                         as cut_uom,
        trim(cut_shape)                       as cut_shape,

        /* Cut numeric dims */
        cast(cut_width as numeric(18,6))      as cut_width,
        cast(cut_length as numeric(18,6))     as cut_length,

        trim(ordered_uom)                     as ordered_uom,
        trim(uom)                              as uom,

        /* Dates */
        cast(order_date as date)              as order_date,
        cast(invoice_date as date)            as invoice_date,
        cast(actual_ship_date as date)        as actual_ship_date,
        cast(quote_date as date)              as quote_date,
        cast(request_date as date)            as request_date,
        cast(promise_date as date)            as promise_date,

        /* Numeric financials */
        cast(ordered_qty as numeric(18,6))            as ordered_qty,
        cast(ordered_pcs as numeric(18,6))            as ordered_pcs,
        cast(freight_cost as numeric(18,4))           as freight_cost,
        cast(freight_revenue_usd as numeric(18,4))    as freight_revenue_usd,
        cast(material_revenue as numeric(18,4))       as material_revenue,
        cast(proc_rev_usd as numeric(18,4))           as proc_rev_usd,
        cast(material_cost_aac as numeric(18,4))      as material_cost_aac,
        cast(material_overhead_cost as numeric(18,4)) as material_overhead_cost,
        cast(outside_processing_cost as numeric(18,4)) as outside_processing_cost,
        cast(resource_cost_usd as numeric(18,4))      as resource_cost_usd,
        cast(total_gross_profit_usd as numeric(18,4)) as total_gross_profit_usd,
        cast(list_price_per_lbs_gross as numeric(18,6)) as list_price_per_lbs_gross,
        cast(price_per_lbs_gross as numeric(18,6))      as price_per_lbs_gross,
        cast(total_sales_usd as numeric(18,4))          as total_sales_usd,
        cast(weight_lbs as numeric(18,4))               as weight_lbs,
        cast(gross_weight_lbs as numeric(18,4))         as gross_weight_lbs,
        cast(invoiced_lbs as numeric(18,4))             as invoiced_lbs,
        cast(invoiced_pcs as numeric(18,4))             as invoiced_pcs,
        cast(invoiced_qty as numeric(18,4))             as invoiced_qty,
        cast(ordered_lbs as numeric(18,4))              as ordered_lbs,
        cast(matl_gp_usd as numeric(18,4))              as matl_gp_usd,
        cast(tgp_pct as numeric(18,6))                   as tgp_pct,
        cast(mgp_pct as numeric(18,6))                   as mgp_pct,
        cast(absorption_cost_usd as numeric(18,4))      as absorption_cost_usd,

        /* Product attributes */
        trim(product_item_nbr)                as product_item_nbr,
        trim(product_item_type)               as product_item_type,
        trim(product_primary_item_nbr)        as product_primary_item_nbr,
        trim(product_form)                    as product_form,
        trim(product_grade)                   as product_grade,
        trim(product_customer)                as product_customer,
        trim(product_shape)                   as product_shape,
        trim(product_stocking_uom)            as product_stocking_uom,
        trim(product_primary_dimension)       as product_primary_dimension,
        trim(product_temper)                  as product_temper,

        cast(product_length as numeric(18,6)) as product_length,
        cast(product_width  as numeric(18,6)) as product_width,

        trim(product_commodity)               as product_commodity,
        trim(product_item_description)        as product_item_description,
        trim(product_source_type)             as product_source_type,

        trim(ship_to_customer_name)           as ship_to_customer_name,
        trim(ship_to_customer_nbr)            as ship_to_customer_nbr,
        trim(sold_to_customer_name)           as sold_to_customer_name,
        trim(sold_to_customer_nbr)            as sold_to_customer_nbr

    from raw
)

select * from cleaned
