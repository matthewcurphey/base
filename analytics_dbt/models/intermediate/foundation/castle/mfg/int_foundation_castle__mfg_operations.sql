{{ config(materialized='view') }}

with src as (

    select *
    from {{ ref('int_foundation_stgcastledj_fxwpl') }}

),

-- 1️⃣ Roll up to discrete job + operation level
productionorder_rows as (

    select
        'castle'                    as company,
        max(org)                    as org,
        discrete_job_no             as dj_nbr,
        operation_sequence_number   as operation_sequence,
        operation_code              as operation_code,

        sum(hrs_earned)             as raw_earned_hrs,
        sum(applied_resource_value) as resource_cost

    from src
    group by
        discrete_job_no,
        operation_sequence_number,
        operation_code
)

select *
from productionorder_rows
