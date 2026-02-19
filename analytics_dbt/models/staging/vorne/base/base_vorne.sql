{{ config(materialized='view') }}

with raw as (

    select *
    from {{ source('vorne', 'vorne') }}

),

cleaned as (

    select

        -- identity
        trim(ip_address)                               as ip_address,
        trim(asset_name)                               as asset_name,
        cast(event_id as integer)                      as event_id,
        trim(sync_id)                                  as sync_id,
        trim(information_source)                       as info_source,

        -- time
        cast(duration as numeric(18,6))                 as duration_sec,
        trim(end_time)                                  as end_datetime,
        trim(shift_hour_display_name)                   as shift,


        -- machine status
        trim(planned_stop_time)                         as machine_status,
        trim(process_state_event_id)                    as machine_event,
        trim(production_day_display_name)               as machine_reason,
        trim(production_phase_event_id)                 as machine_phase,



        -- cycles/parts
        cast(equipment_cycles as integer)                as equipment_cycles,
        cast(expected_count as integer)                  as expected_count,
        cast(good_count as integer)                      as good_count,
        cast(in_count as integer)                        as in_count,
        cast(pack_out_count as integer)                  as pack_out_count,

        trim(part)                                       as part,
        trim(part_event_id)                              as part_event_id

    from raw

)

select *
from cleaned