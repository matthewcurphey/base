{{ config(materialized='view') }}

-- 1) Pull raw ERP data exactly as-is
with raw as (

    select
        "dataAreaId"                        as company,
        "SalesOrderNumber"                  as sales_order_number,
        "SalesOrderStatus"                  as sales_order_status,
        "SalesOrderProcessingStatus"        as sales_order_processing_status,
        "SalesOrderName"                    as sales_order_name,
        "CustomerRequisitionNumber"         as customer_requisition_number,
        "CustomersOrderReference"           as customer_order_reference,
        "InvoiceCustomerAccountNumber"      as invoice_customer_account_number,
        "OrderTotalAmount"                  as order_total_amount,
        "OrderTotalChargesAmount"           as order_total_charges_amount,
        "OrderTotalTaxAmount"               as order_total_tax_amount,
        "CurrencyCode"                      as currency_code,

        -- Dates (timestamp-like)
        "OrderCreationDateTime"             as order_creation_datetime,
        "RequestedReceiptDate"              as requested_receipt_date,
        "ConfirmedReceiptDate"              as confirmed_receipt_date,
        "RequestedShippingDate"             as requested_shipping_date,
        "ConfirmedShippingDate"             as confirmed_shipping_date,

        -- Customer
        "Email"                             as email,

        -- Delivery address
        "DeliveryAddressName"               as delivery_address_name,
        "DeliveryAddressStreet"             as delivery_address_street,
        "DeliveryAddressCity"               as delivery_address_city,
        "DeliveryAddressStateId"            as delivery_address_state,
        "DeliveryAddressZipCode"            as delivery_address_zipcode,
        "DeliveryAddressCountryRegionId"    as delivery_address_country,

        -- Logistics
        "DeliveryModeCode"                  as delivery_mode_code,
        "DefaultShippingSiteId"             as default_shipping_site_id,
        "DefaultShippingWarehouseId"        as default_shipping_warehouse_id

    from {{ source('banner', 'banner_salesorderheaders') }}

),

-- 2) Trim + Normalize Types
cleaned as (

    select
        -- Identifiers
        lower(trim(company))                    as company,
        trim(sales_order_number)                as sales_order_number,
        trim(sales_order_status)                as sales_order_status,
        trim(sales_order_processing_status)     as sales_order_processing_status,
        trim(sales_order_name)                  as sales_order_name,

        -- Customer / reference info
        trim(customer_requisition_number)       as customer_requisition_number,
        trim(customer_order_reference)          as customer_order_reference,
        trim(invoice_customer_account_number)   as invoice_customer_account_number,

        -- Financial totals
        cast(order_total_amount as numeric(18,2))        as order_total_amount,
        cast(order_total_charges_amount as numeric(18,2)) as order_total_charges_amount,
        cast(order_total_tax_amount as numeric(18,2))      as order_total_tax_amount,

        trim(currency_code)                    as currency_code,

        -- Dates (standardised to date)
        cast(order_creation_datetime as date)      as order_creation_date,
        cast(requested_receipt_date as date)       as requested_receipt_date,
        cast(confirmed_receipt_date as date)       as confirmed_receipt_date,
        cast(requested_shipping_date as date)      as requested_shipping_date,
        cast(confirmed_shipping_date as date)      as confirmed_shipping_date,

        -- Customer info
        trim(email)                             as email,

        -- Delivery address
        trim(delivery_address_name)             as delivery_address_name,
        trim(delivery_address_street)           as delivery_address_street,
        trim(delivery_address_city)             as delivery_address_city,
        trim(delivery_address_state)            as delivery_address_state,
        trim(delivery_address_zipcode)          as delivery_address_zipcode,
        trim(delivery_address_country)          as delivery_address_country,

        -- Logistics
        trim(delivery_mode_code)                as delivery_mode_code,
        trim(default_shipping_site_id)          as default_shipping_site_id,
        trim(default_shipping_warehouse_id)     as default_shipping_warehouse_id

    from raw
    where sales_order_number is not null
)

select * from cleaned