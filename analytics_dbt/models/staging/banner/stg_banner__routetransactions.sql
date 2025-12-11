{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_banner__routetransactions') }}
),

staged as (

    select
        -- Identifiers
        company::text                            as company,
        production_order_number::text            as production_order_number,
        operation_number::text                   as operation_number,
        operation_id::text                       as operation_id,
        route_operation_sequence::text           as route_operation_sequence,

        -- Dates
        scheduled_from_date::date                as scheduled_from_date,
        scheduled_end_date::date                 as scheduled_end_date,

        -- Quantities / Times
        process_quantity::numeric(18,6)          as process_quantity,
        estimated_operation_quantity::numeric(18,6) as estimated_operation_quantity,
        process_time::numeric(18,6)              as process_time,
        estimated_process_time::numeric(18,6)    as estimated_process_time

    from base
)

select * from staged