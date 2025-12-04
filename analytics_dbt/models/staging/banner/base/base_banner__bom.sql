{{ config(materialized='view') }}

-- 1) Pull raw ERP data exactly as-is
with raw as (

    select
        "ProductionOrderNumber",
        "ItemNumber",
        "LineNumber",
        "BOMLineQuantity",
        "BOMLineQuantityDenominator",
        "StartedInventoryQuantity",
        "StartedBOMLineQuantity",
        "RemainingBOMLineQuantity",
        "RemainingInventoryQuantity",
        "ReleasedBOMLineQuantity",
        "SourceBOMId",
        "EstimatedBOMLineQuantity",
        "ProductSizeId",
        "dataAreaId"

    from {{ source('banner', 'banner_bom') }}
),

-- 2) Normalise columns, fix types, trim junk
cleaned as (

    select
        -- Keys / identifiers
        lower(trim("dataAreaId"))                      as company,
        trim("ProductionOrderNumber")                  as production_order_number,
        trim("ItemNumber")                             as item_number,
        trim("LineNumber")                             as line_number,

        -- Quantities
        cast("BOMLineQuantity" as numeric)             as bom_line_quantity,
        cast("BOMLineQuantityDenominator" as numeric)  as bom_line_quantity_denominator,
        cast("StartedInventoryQuantity" as numeric)    as started_inventory_quantity,
        cast("StartedBOMLineQuantity" as numeric)      as started_bom_line_quantity,
        cast("RemainingBOMLineQuantity" as numeric)    as remaining_bom_line_quantity,
        cast("RemainingInventoryQuantity" as numeric)  as remaining_inventory_quantity,
        cast("ReleasedBOMLineQuantity" as numeric)     as released_bom_line_quantity,
        cast("EstimatedBOMLineQuantity" as numeric)    as estimated_bom_line_quantity,

        -- Metadata / BOM header reference
        trim("SourceBOMId")                            as source_bom_id,
        trim("ProductSizeId")                          as grade

    from raw
    where "ProductionOrderNumber" is not null

)

select * from cleaned
