{{ config(materialized='table') }}

/* =====================================================
   DAILY TARGET PERFORMANCE
   One row per org/day: target, backlog composition (no_material vs.
   with_material, the latter split in/out of the 7-day lead time), actual
   shipped lines, and performance-to-target as continuous measures
   (variance, pct) rather than a Hit/Miss verdict — too binary for what
   was actually asked (performance to target, not a pass/fail label).
   Daily only — no trailing-week measure.

   variance_to_target / pct_to_target answer ONE question always:
   performance against the stated target. They go null — not some
   adjusted/curved number — on any day that comparison isn't a fair one:
   not finished yet, not a business day, no target set for this org, no
   material snapshot yet, or with_material itself was below target that
   day. That last case is a real non-judgment, same as the old "Not
   Enough Material" verdict state: if the material ceiling was already
   under target, shipping performance isn't the thing that failed, so
   it isn't scored. The material picture is never hidden — it's right
   there in no_material/with_material on the same row — it's just not
   blended into the same number.

   with_material_in_lt / with_material_out_lt are purely descriptive: of
   the lines with material, how many are still within McMaster's
   standard 7-day lead time vs. already past it. Never used to gate or
   adjust the score.
===================================================== */

with actual as (

    select
        dt,
        inv_org_code,
        open_orders as backlog,
        shipped_orders,
        is_business_day
    from {{ ref('mart_mcmaster__backlog_daily') }}

),

material as (

    select
        snapshot_date,
        inv_org_code,
        sum(case when mcm_status = 'No Material' then line_count else 0 end)
            as no_material,
        sum(case when mcm_status != 'No Material' then line_count else 0 end)
            as with_material,
        sum(case when mcm_status != 'No Material' and lt_bucket = 'in_lead_time' then line_count else 0 end)
            as with_material_in_lt,
        sum(case when mcm_status != 'No Material' and lt_bucket = 'out_of_lead_time' then line_count else 0 end)
            as with_material_out_lt
    from {{ ref('mart_mcmaster__material_availability_daily') }}
    group by snapshot_date, inv_org_code

),

targeted as (

    select
        a.dt,
        a.inv_org_code,
        a.backlog,
        a.shipped_orders,
        a.is_business_day,
        t.target_lines_per_day
    from actual a
    left join lateral (
        select target_lines_per_day
        from {{ ref('ref_mcmaster_daily_targets') }} rt
        where rt.inv_org_code = a.inv_org_code
          and rt.effective_from <= a.dt
        order by rt.effective_from desc
        limit 1
    ) t on true

),

combined as (

    select
        tg.*,
        m.no_material,
        m.with_material,
        m.with_material_in_lt,
        m.with_material_out_lt
    from targeted tg
    left join material m
        on  m.inv_org_code   = tg.inv_org_code
        and m.snapshot_date  = tg.dt

)

select
    c.dt,
    c.inv_org_code,
    c.target_lines_per_day                                     as target,
    c.backlog,
    c.no_material,
    c.with_material,
    c.with_material_in_lt,
    c.with_material_out_lt,

    -- Supply-side fact, not a performance measure: what share of the whole
    -- open backlog currently has material, regardless of target. Null
    -- follows naturally from with_material being null (no snapshot yet) —
    -- no separate gating needed, unlike variance/pct_to_target below.
    round(100.0 * c.with_material / nullif(c.backlog, 0), 1)   as material_availability_pct,

    c.shipped_orders                                           as shipped,

    case
        when c.dt >= current_date                              then null
        when not c.is_business_day                              then null
        when c.target_lines_per_day is null                     then null
        when c.with_material is null                            then null
        when c.with_material < c.target_lines_per_day           then null
        else c.shipped_orders - c.target_lines_per_day
    end                                                         as variance_to_target,

    case
        when c.dt >= current_date                              then null
        when not c.is_business_day                              then null
        when c.target_lines_per_day is null                     then null
        when c.with_material is null                            then null
        when c.with_material < c.target_lines_per_day           then null
        else round(100.0 * c.shipped_orders / c.target_lines_per_day, 1)
    end                                                         as pct_to_target

from combined c

order by c.dt desc, c.inv_org_code
