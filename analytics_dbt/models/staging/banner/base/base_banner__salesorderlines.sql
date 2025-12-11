{{ config(materialized='view') }}

-- 1) Pull raw ERP data exactly as-is
with raw as (

    select
        "dataAreaId"                  as company,
        "InventoryLotId"              as inventory_lot_id,
        "SalesUnitSymbol"             as sales_unit_symbol,
        "ShippingSiteId"              as shipping_site_id,
        "LineNumber"                  as line_number,
        "LineDescription"             as line_description,
        "ItemNumber"                  as item_number,
        "ShippingWarehouseId"         as shipping_warehouse_id,
        "RequestedReceiptDate"        as requested_receipt_date,
        "OrderedSalesQuantity"        as ordered_sales_quantity,
        "LineAmount"                  as line_amount,
        "ProductSizeId"               as product_grade,
        "SalesPrice"                  as sales_price,
        "SalesOrderNumber"            as sales_order_number,
        "SalesOrderLineStatus"        as sales_order_line_status,
        "DeliveryAddressDescription"  as delivery_address_description,
        "RequestedShippingDate"       as requested_shipping_date,
        "DeliveryModeCode"            as delivery_mode_code,
        "CustomersLineNumber"         as customers_line_number,
        "CurrencyCode"                as currency_code

    from {{ source('banner', 'banner_salesorderlines') }}

),

-- 2) Trim + Normalize Types
cleaned as (

    select
        -- Identifiers
        lower(trim(company))                         as company,
        trim(sales_order_number)                     as sales_order_number,
        trim(item_number)                            as item_number,
        trim(line_number)                            as line_number,
        trim(customers_line_number)                  as customers_line_number,
        trim(inventory_lot_id)                       as inventory_lot_id,

        -- Dates
        cast(requested_receipt_date as date)         as requested_receipt_date,
        cast(requested_shipping_date as date)        as requested_shipping_date,

        -- Quantities and pricing
        cast(ordered_sales_quantity as numeric(18,6)) as ordered_sales_quantity,
        cast(sales_price as numeric(18,6))            as sales_price,
        cast(line_amount as numeric(18,2))            as line_amount,

        -- Metadata
        trim(sales_order_line_status)                as sales_order_line_status,
        trim(line_description)                       as line_description,
        trim(product_grade)                        as product_grade,
        trim(currency_code)                          as currency_code,

        -- Logistics
        trim(sales_unit_symbol)                      as sales_unit_symbol,
        trim(shipping_site_id)                       as shipping_site_id,
        trim(shipping_warehouse_id)                  as shipping_warehouse_id,
        trim(delivery_address_description)           as delivery_address_description,
        trim(delivery_mode_code)                     as delivery_mode_code

    from raw
    where sales_order_number is not null
)

select * from cleaned