{{ config(materialized='view') }}

-- 1) Pull raw ERP data exactly as-is
with raw as (

    select
        "dataAreaId"                 as company,
        "ProductionOrderNumber"      as production_order_number,
        "OperationNumber"            as operation_number,
        "ProcessQuantity"            as process_quantity,
        "EstimatedProcessTime"       as estimated_process_time,
        "ScheduledFromDate"          as scheduled_from_date,
        "ScheduledEndDate"           as scheduled_end_date,
        "EstimatedOperationQuantity" as estimated_operation_quantity,
        "OperationId"                as operation_id,
        "RouteOperationSequence"     as route_operation_sequence,
        "ProcessTime"                as process_time

    from {{ source('banner', 'banner_routetransactions') }}

),

-- 2) Trim + Normalize Types
cleaned as (

    select
        -- Keys & identifiers
        lower(trim(company))                        as company,
        trim(production_order_number)               as production_order_number,
        cast(operation_number as integer)                      as operation_number,
        trim(operation_id)                          as operation_id,
        cast(route_operation_sequence as integer)              as route_operation_sequence,

        -- Dates
        cast(scheduled_from_date as date)           as scheduled_from_date,
        cast(scheduled_end_date as date)            as scheduled_end_date,

        -- Quantities & times
        cast(process_quantity as numeric(18,6))     as process_quantity,
        cast(estimated_operation_quantity as numeric(18,6)) as estimated_operation_quantity,
        cast(process_time as numeric(18,6))         as process_time,
        cast(estimated_process_time as numeric(18,6)) as estimated_process_time

    from raw
    where production_order_number is not null
)

select * from cleaned