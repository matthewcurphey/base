{{ config(materialized='table') }}

with backlog as (

    select * from {{ ref('int_oracle__mcmaster_02_open_backlog') }}

),

/* =====================================================
   SHORT LINES
   Individual lines with material shortfall, ranked
   smallest → largest within item + org so we can
   maximise lines cleared per unit spent.
===================================================== */

short_lines as (

    select
        item_clean,
        inv_org_code                                            as short_org,
        comp_inv_req,
        sum(comp_inv_req) over (
            partition by item_clean, inv_org_code
            order by comp_inv_req asc nulls last
            rows unbounded preceding
        )                                                       as cumulative_req
    from backlog
    where is_short

),

/* =====================================================
   SHORTFALL SUMMARY
   Per item + org: total unmet demand and line count.
===================================================== */

shortfall as (

    select
        item_clean,
        inv_org_code                                            as short_org,
        unit_of_measure,
        count(*)                                                as lines_short,
        sum(comp_inv_req)                                       as total_short_qty
    from backlog
    where is_short
    group by
        item_clean,
        inv_org_code,
        unit_of_measure

),

/* =====================================================
   DONORS
   Orgs with a positive floor for this item — meaning
   they have genuine surplus after all their own demand.
===================================================== */

donors as (

    select distinct
        item_clean,
        inv_org_code                                            as donor_org,
        case inv_org_code
            when 'ATL' then floor_atl
            when 'CLE' then floor_cle
            when 'DAL' then floor_dal
            when 'JVL' then floor_jvl
            when 'LOS' then floor_los
            when 'WIE' then floor_wie
        end                                                     as donor_floor
    from backlog
    where case inv_org_code
            when 'ATL' then floor_atl
            when 'CLE' then floor_cle
            when 'DAL' then floor_dal
            when 'JVL' then floor_jvl
            when 'LOS' then floor_los
            when 'WIE' then floor_wie
          end > 0

),

/* =====================================================
   LINES COVERABLE
   For each item + short_org + donor_org combo:
   count how many lines fit within the donor's floor
   working smallest → largest (maximise lines cleared).
===================================================== */

lines_coverable as (

    select
        sl.item_clean,
        sl.short_org,
        d.donor_org,
        count(*)                                                as lines_coverable,
        max(sl.cumulative_req)                                  as qty_needed
    from short_lines sl
    inner join donors d
        on  sl.item_clean = d.item_clean
        and sl.short_org  != d.donor_org
    where sl.cumulative_req <= d.donor_floor
    group by
        sl.item_clean,
        sl.short_org,
        d.donor_org

),

/* =====================================================
   DONOR LOAD
   Total shortfall demand pointing at each donor across
   all short orgs for this item. Flags overcommitment —
   if combined demand > donor floor, moves are mutually exclusive.
===================================================== */

donor_load as (

    select
        d.item_clean,
        d.donor_org,
        sum(s.total_short_qty)                                  as total_demand_on_donor
    from donors d
    inner join shortfall s
        on  d.item_clean  = s.item_clean
        and d.donor_org   != s.short_org
    group by
        d.item_clean,
        d.donor_org

)

/* =====================================================
   FINAL MATRIX
   Every short org × donor org pair for the same item.
   Sorted: full line cover first, non-contingent before
   contingent, then most lines coverable.
===================================================== */

select
    s.item_clean,
    s.unit_of_measure,
    s.short_org,
    s.lines_short,
    s.total_short_qty,
    d.donor_org,
    d.donor_floor                                               as donor_available_qty,
    coalesce(lc.lines_coverable, 0)                             as lines_coverable,
    coalesce(lc.qty_needed, 0)                                  as qty_needed,
    round(
        coalesce(lc.lines_coverable, 0)::numeric
        / nullif(s.lines_short, 0) * 100
    , 1)                                                        as lines_coverage_pct,
    coalesce(lc.lines_coverable, 0) = s.lines_short             as full_lines_cover,
    dl.total_demand_on_donor > d.donor_floor                    as donor_overcommitted

from shortfall s
inner join donors d
    on  s.item_clean  = d.item_clean
    and s.short_org   != d.donor_org
left join lines_coverable lc
    on  s.item_clean  = lc.item_clean
    and s.short_org   = lc.short_org
    and d.donor_org   = lc.donor_org
inner join donor_load dl
    on  d.item_clean  = dl.item_clean
    and d.donor_org   = dl.donor_org

order by
    full_lines_cover        desc,
    donor_overcommitted     asc,
    lines_coverable         desc,
    lines_coverage_pct      desc
