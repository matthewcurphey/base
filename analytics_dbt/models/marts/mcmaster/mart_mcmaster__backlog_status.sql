{{ config(materialized='table') }}

select

    inv_org_code,
    mcm_status,
    line_count,
    total_usd

from {{ ref('int_oracle__mcmaster_metric_backlog_status') }}
