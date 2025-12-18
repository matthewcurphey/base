{{ config(materialized='view') }}

-- 1) Pull raw ERP data exactly as-is
with raw as (

    select
        -- Production Order Headers (P)
        P."dataAreaId"                           as company,
        P."ProductionOrderNumber"                as production_order_number,
        P."ItemNumber"                           as item_number,
        P."ProductionOrderStatus"                as production_order_status,
        P."StartedDate"                          as started_date,
        P."DeliveryDate"                         as delivery_date,
        P."ProductSizeId"                        as grade,
        P."DemandSalesOrderNumber"               as demand_sales_order_number,
        P."SourceBOMVersionValidityDate"         as source_bom_version_validity_date,
        P."EstimatedQuantity"                    as estimated_quantity,
        P."EndedDate"                            as ended_date,
        P."ProductionSiteId"                     as production_site_id,
        P."SourceBOMId"                          as source_bom_id,
        P."DemandSalesOrderLineInventoryLotId"   as demand_sales_order_line_lot_id,
        P."DemandProductionOrderLineNumber"      as demand_production_order_line_number,

        -- Joined Sales Order Line Information (S)
        S."LineAmount"                           as line_amount,
        S."LineNumber"                           as sales_line_number,
        S."OrderedSalesQuantity"                 as ordered_sales_quantity,
        S."SalesUnitSymbol"                      as sales_unit_symbol

    from {{ source('banner', 'banner_productionorders') }} as P
    left join {{ source('banner', 'banner_salesorderlines') }} as S
        on P."dataAreaId" = S."dataAreaId"
        and P."DemandSalesOrderLineInventoryLotId" = S."InventoryLotId"

),

-- 2) Trim + Normalize Types
cleaned as (

    select
        -- Keys & identifiers
        lower(trim(company))                         as company,
        trim(production_order_number)                as production_order_number,
        trim(item_number)                            as item_number,
        trim(production_order_status)                as production_order_status,

        -- Dates (normalized to date)
        cast(started_date as date)                   as started_date,
        cast(delivery_date as date)                  as delivery_date,
        cast(ended_date as date)                     as ended_date,
        cast(source_bom_version_validity_date as date) as source_bom_version_validity_date,

        -- Demand info
        trim(demand_sales_order_number)              as demand_sales_order_number,
        trim(demand_sales_order_line_lot_id)         as demand_sales_order_line_lot_id,
        trim(demand_production_order_line_number)    as demand_production_order_line_number,

        -- Quantities
        cast(estimated_quantity as numeric(18,6))    as estimated_quantity,
        cast(ordered_sales_quantity as numeric(18,6)) as ordered_sales_quantity,

        -- Sales line join fields
        trim(sales_line_number)                      as sales_line_number,
        cast(line_amount as numeric(18,2))           as line_amount,
        trim(sales_unit_symbol)                      as sales_unit_symbol,

        -- Metadata fields
        trim(grade)                        as grade,
        trim(production_site_id)                     as production_site_id,
        trim(source_bom_id)                          as source_bom_id

    from raw
    where production_order_number is not null
)

select * from cleaned