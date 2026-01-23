{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_banner__routetransactions') }}
),

-- 1) Roll up to production order × operation level
operation_rows as (
    select
        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        company,
        production_order_number,
        operation_number,
        operation_id,


        /* =======================
           SCHEDULE DATES
           ======================= */
        min(scheduled_from_date)               as scheduled_from_date,
        max(scheduled_end_date)                as scheduled_end_date,

        /* =======================
           QUANTITIES
           ======================= */
        max(process_quantity)                  as process_quantity,
        max(estimated_operation_quantity)      as estimated_operation_quantity,

        /* =======================
           TIME / EFFORT
           ======================= */
        sum(process_time)                      as process_time,
        sum(estimated_process_time)            as estimated_process_time

    from src
    group by
        company,
        production_order_number,
        operation_number,
        operation_id
)

select *
from operation_rows