{{ config(materialized='view') }}

WITH base AS (

    SELECT *
    FROM {{ ref('base_vorne') }}

),

-- Remove Detecting State first
filtered AS (

    SELECT *
    FROM base
    WHERE machine_event <> 'Detecting State'

),

-- Assign machine status priority
prioritized AS (

    SELECT
        *,

        CASE machine_status
            WHEN 'Run'            THEN 1
            WHEN 'Planned Stop'   THEN 2
            WHEN 'Unplanned Stop' THEN 3
            WHEN 'Not Scheduled'  THEN 4
            ELSE 99
        END AS status_priority

    FROM filtered

),

-- Deterministic dedupe
ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ip_address, event_id
            ORDER BY status_priority ASC
        ) AS rn
    FROM prioritized

),

staged AS (

    SELECT
        ip_address::text                AS ip_address,
        asset_name::text                AS asset_name,
        event_id::integer               AS event_id,
        sync_id::text                   AS sync_id,
        info_source::text               AS info_source,

        duration_sec::numeric(18,6)     AS duration_sec,
        end_datetime::timestamp         AS end_datetime,
        shift::text                     AS shift,

        machine_status::text            AS machine_status,
        machine_event::text             AS machine_event,
        machine_reason::text            AS machine_reason,
        machine_phase::text             AS machine_phase,

        equipment_cycles::integer       AS equipment_cycles,
        expected_count::integer         AS expected_count,
        good_count::integer             AS good_count,
        in_count::integer               AS in_count,
        pack_out_count::integer         AS pack_out_count,

        part::text                      AS part,
        part_event_id::text             AS part_event_id

    FROM ranked
    WHERE rn = 1

)

SELECT *
FROM staged