{{ config(materialized='table') }}

with backlog as (

    select * from {{ ref('int_oracle__mcmaster_02_open_backlog') }}
    where is_mcmaster

)

/* =====================================================
   OPEN BACKLOG SUMMARY
   McMaster lines only, status x org grain — line count
   and USD total for each combination.
===================================================== */

select
    inv_org_code,
    mcm_status,
    count(*)                    as line_count,
    sum(total_sales_usd)        as total_usd

from backlog
group by
    inv_org_code,
    mcm_status

order by
    inv_org_code,
    mcm_status
