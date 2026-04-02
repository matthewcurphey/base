{{ config(materialized='view') }}

-- Cleaned HR worked hours at employee / org / year / month grain
-- Excludes rows where total_hrs = 0 (mirrors Python ETL filter)

select *
from {{ ref('base_hr__worked_hrs') }}
where total_hrs != 0
