{{ config(materialized='table') }}

-- Castle productivity results at org / year / month grain
-- productivity_pct = earned_hrs / worked_total_hrs
-- Payout resolved via ref_piptargettimings → ref_piptargets:
--   1. Map year/month to the correct target year (e.g. Jan 2026 uses 2025 targets)
--   2. Join to all target bands where productivity_pct >= goal_pct
--   3. Keep the highest qualifying band (max band_pct)

with earned_hrs as (

    select *
    from {{ ref('int_castle__productivity_01_earnedhrs') }}

),

worked_hrs as (

    select *
    from {{ ref('int_foundation_hr__worked_hrs_summary') }}

),

pip_timings as (

    select *
    from {{ ref('ref_piptargettimings') }}

),

pip_targets as (

    select *
    from {{ ref('ref_piptargets') }}

),

pip_overrides as (

    select *
    from {{ ref('ref_pip_overrides') }}

),

-- Target: goal_pct for band_pct = 1 (the official 100% target)
entry_thresholds as (

    select org, year, goal_pct as target_pct
    from {{ ref('ref_piptargets') }}
    where band_pct = 1

),

-- 1️⃣ Merge worked + earned hours and calculate productivity%
combined as (

    select
        w.country,
        w.org,
        w.year,
        w.month,
        w.worked_reg_hrs,
        w.worked_ot_hrs,
        w.worked_total_hrs,
        coalesce(e.earned_hrs, 0)                                          as earned_hrs,
        coalesce(e.earned_hrs, 0) / nullif(w.worked_total_hrs, 0)          as productivity_pct

    from worked_hrs w
    left join earned_hrs e
        on  w.org   = e.org
        and w.year  = e.year
        and w.month = e.month

),

-- 2️⃣ Resolve which PIP target year applies for each year/month
with_target_year as (

    select
        c.*,
        pt.target_year_used

    from combined c
    left join pip_timings pt
        on  c.year  = pt.year
        and c.month = pt.month

),

-- 3️⃣ Join to all qualifying PIP target bands
--    Qualifying = productivity_pct >= goal_pct for the org + target year
--    Row number ranks bands desc so band_rank = 1 is the highest qualifying band
banded as (

    select
        w.country,
        w.org,
        w.year,
        w.month,
        w.target_year_used,
        w.worked_reg_hrs,
        w.worked_ot_hrs,
        w.worked_total_hrs,
        w.earned_hrs,
        w.productivity_pct,
        t.band_pct,
        t.payout_usd,
        t.uom,

        row_number() over (
            partition by w.org, w.year, w.month
            order by t.band_pct desc
        ) as band_rank

    from with_target_year w
    left join pip_targets t
        on  w.org              = t.org
        and w.target_year_used = t.year
        and w.productivity_pct >= t.goal_pct

)

select
    b.country,
    b.org,
    b.year,
    b.month,
    b.worked_reg_hrs,
    b.worked_ot_hrs,
    b.worked_total_hrs,
    b.earned_hrs,
    et.target_pct,
    b.productivity_pct,
    b.band_pct,
    coalesce(o.payout_usd, b.payout_usd) as payout_usd,
    b.uom

from banded b
left join entry_thresholds et
    on  b.org              = et.org
    and b.target_year_used = et.year
left join pip_overrides o
    on  b.org   = o.org
    and b.year  = o.year
    and b.month = o.month

where b.band_rank = 1
  and b.org in ('ASC','ATL','CLE','DAL','ENA','ENT','HAI','JVL','LOS','MCH','MTY','MXM','MXQ','SGP','STO','TOR','WIE')
