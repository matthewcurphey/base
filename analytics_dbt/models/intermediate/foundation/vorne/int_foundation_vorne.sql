{{ config(materialized='view') }}

with src as (

    select *
    from {{ ref('stg_vorne') }}

),

base as (

    select
        *,
        -- derive start time
        end_datetime - (duration_sec * interval '1 second') 
            as start_datetime
    from src
    where duration_sec > 0 and duration_sec < 10000

),

expanded as (

    select
        b.*,

        generate_series(
            date_trunc('day', start_datetime),
            date_trunc('day', end_datetime),
            interval '1 day'
        )::date as event_date

    from base b

),

allocated as (

    select
        *,
        
        -- allocate overlapping portion
        extract(
            epoch from
            least(end_datetime, event_date + interval '1 day')
            -
            greatest(start_datetime, event_date)
        ) as allocated_duration_sec

    from expanded

)

select *
from allocated
where allocated_duration_sec > 0