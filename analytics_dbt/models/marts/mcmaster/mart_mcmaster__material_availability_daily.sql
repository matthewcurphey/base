{{ config(
    materialized='incremental',
    unique_key=['snapshot_date', 'inv_org_code', 'mcm_status', 'lt_bucket'],
    incremental_strategy='delete+insert'
) }}

/* =====================================================
   DAILY MATERIAL AVAILABILITY LOG
   mcm_status (int_oracle__mcmaster_02_open_backlog) has no date
   dimension at all — it's computed from live inventory tallies and job
   status as they exist at query time, so it only ever describes "right
   now." There is no way to reconstruct a past day's material picture
   after the fact — the backlog composition and inventory tallies have
   already moved on. This model is the only place that picture ever gets
   captured: whatever a run sees today becomes today's permanent record,
   stamped with snapshot_date and never overwritten by a later run.

   Sources line-level detail directly from int_oracle__mcmaster_02_open_backlog
   (rather than the pre-aggregated int_oracle__mcmaster_metric_backlog_status,
   which has no order_date) so lt_bucket can be derived here: order age vs.
   McMaster's standard 7-day lead time, as of this snapshot. Descriptive only
   — mart_mcmaster__daily_target_performance never gates its score on this,
   only on total material availability.

   is_incremental() guards against inserting a duplicate snapshot if
   dbt is run more than once on the same day; unique_key +
   delete+insert is a second line of defense for the same case.
===================================================== */

with backlog as (

    select *
    from {{ ref('int_oracle__mcmaster_02_open_backlog') }}
    where is_mcmaster

),

bucketed as (

    select
        current_date                                              as snapshot_date,
        inv_org_code,
        mcm_status,
        case
            when current_date - order_date < 7 then 'in_lead_time'
            else 'out_of_lead_time'
        end                                                        as lt_bucket,
        total_sales_usd
    from backlog

    {% if is_incremental() %}
    where current_date not in (select distinct snapshot_date from {{ this }})
    {% endif %}

)

select
    snapshot_date,
    inv_org_code,
    mcm_status,
    lt_bucket,
    count(*)               as line_count,
    sum(total_sales_usd)   as total_usd
from bucketed
group by snapshot_date, inv_org_code, mcm_status, lt_bucket
