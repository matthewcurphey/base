{{ config(materialized='table') }}

with backlog as (

    select * from {{ ref('int_castle__mcmaster_02_open_backlog') }}

),

po as (

    select * from {{ ref('int_foundation_castle__po_open') }}

),

/* =====================================================
   SHORTFALL BY ITEM + ORG
   Pivot to one row per item with per-org columns.
   Only items where at least one org has short lines.
===================================================== */

shortfall as (

    select
        product_primary_item_nbr,
        max(inv_uom)                                                                as inv_uom,

        sum(case when inv_org_code = 'ATL' and is_short then 1          else 0 end) as lines_short_atl,
        sum(case when inv_org_code = 'CLE' and is_short then 1          else 0 end) as lines_short_cle,
        sum(case when inv_org_code = 'DAL' and is_short then 1          else 0 end) as lines_short_dal,
        sum(case when inv_org_code = 'JVL' and is_short then 1          else 0 end) as lines_short_jvl,
        sum(case when inv_org_code = 'LOS' and is_short then 1          else 0 end) as lines_short_los,
        sum(case when inv_org_code = 'WIE' and is_short then 1          else 0 end) as lines_short_wie,

        sum(case when inv_org_code = 'ATL' and is_short then comp_inv_req else 0 end) as lbs_short_atl,
        sum(case when inv_org_code = 'CLE' and is_short then comp_inv_req else 0 end) as lbs_short_cle,
        sum(case when inv_org_code = 'DAL' and is_short then comp_inv_req else 0 end) as lbs_short_dal,
        sum(case when inv_org_code = 'JVL' and is_short then comp_inv_req else 0 end) as lbs_short_jvl,
        sum(case when inv_org_code = 'LOS' and is_short then comp_inv_req else 0 end) as lbs_short_los,
        sum(case when inv_org_code = 'WIE' and is_short then comp_inv_req else 0 end) as lbs_short_wie,

        sum(case when is_short then 1          else 0 end)                          as total_lines_short,
        sum(case when is_short then comp_inv_req else 0 end)                        as total_lbs_short

    from backlog
    where is_short
    group by product_primary_item_nbr

),

/* =====================================================
   PO SUMMARY BY ITEM + ORG
   Pivot open POs to same item grain.
   Company-wide detail concat for full context.
===================================================== */

po_by_org as (

    select
        product_primary_item_nbr,

        count(distinct case when inv_org_code = 'ATL' then po_nbr end)              as po_count_atl,
        count(distinct case when inv_org_code = 'CLE' then po_nbr end)              as po_count_cle,
        count(distinct case when inv_org_code = 'DAL' then po_nbr end)              as po_count_dal,
        count(distinct case when inv_org_code = 'JVL' then po_nbr end)              as po_count_jvl,
        count(distinct case when inv_org_code = 'LOS' then po_nbr end)              as po_count_los,
        count(distinct case when inv_org_code = 'WIE' then po_nbr end)              as po_count_wie,

        sum(case when inv_org_code = 'ATL' then po_open_qty else 0 end)             as po_open_qty_atl,
        sum(case when inv_org_code = 'CLE' then po_open_qty else 0 end)             as po_open_qty_cle,
        sum(case when inv_org_code = 'DAL' then po_open_qty else 0 end)             as po_open_qty_dal,
        sum(case when inv_org_code = 'JVL' then po_open_qty else 0 end)             as po_open_qty_jvl,
        sum(case when inv_org_code = 'LOS' then po_open_qty else 0 end)             as po_open_qty_los,
        sum(case when inv_org_code = 'WIE' then po_open_qty else 0 end)             as po_open_qty_wie,

        min(case when inv_org_code = 'ATL' then po_due_date end)                    as next_po_due_atl,
        min(case when inv_org_code = 'CLE' then po_due_date end)                    as next_po_due_cle,
        min(case when inv_org_code = 'DAL' then po_due_date end)                    as next_po_due_dal,
        min(case when inv_org_code = 'JVL' then po_due_date end)                    as next_po_due_jvl,
        min(case when inv_org_code = 'LOS' then po_due_date end)                    as next_po_due_los,
        min(case when inv_org_code = 'WIE' then po_due_date end)                    as next_po_due_wie,

        count(distinct po_nbr)                                                      as total_po_count,
        sum(po_open_qty)                                                             as total_po_open_qty,
        min(po_due_date)                                                             as next_po_due_anywhere,

        string_agg(
            '[' || inv_org_code || '] '
            || po_nbr
            || ' — ' || vendor_name
            || ' — due ' || to_char(po_due_date, 'DD-Mon-YY')
            || ' — ' || po_open_qty || ' ' || po_uom,
            ' || '
            order by po_due_date asc nulls last
        )                                                                            as po_detail

    from po
    where product_primary_item_nbr in (select product_primary_item_nbr from shortfall)
    group by product_primary_item_nbr

)

select
    s.product_primary_item_nbr,
    s.inv_uom,

    /* per-org shortfall */
    s.lines_short_atl,      s.lbs_short_atl,        p.po_count_atl,     p.po_open_qty_atl,  p.next_po_due_atl,
    s.lines_short_cle,      s.lbs_short_cle,        p.po_count_cle,     p.po_open_qty_cle,  p.next_po_due_cle,
    s.lines_short_dal,      s.lbs_short_dal,        p.po_count_dal,     p.po_open_qty_dal,  p.next_po_due_dal,
    s.lines_short_jvl,      s.lbs_short_jvl,        p.po_count_jvl,     p.po_open_qty_jvl,  p.next_po_due_jvl,
    s.lines_short_los,      s.lbs_short_los,        p.po_count_los,     p.po_open_qty_los,  p.next_po_due_los,
    s.lines_short_wie,      s.lbs_short_wie,        p.po_count_wie,     p.po_open_qty_wie,  p.next_po_due_wie,

    /* company totals */
    s.total_lines_short,
    s.total_lbs_short,
    coalesce(p.total_po_count, 0)                                                   as total_po_count,
    coalesce(p.total_po_open_qty, 0)                                                as total_po_open_qty,
    p.next_po_due_anywhere,
    p.total_po_count is null                                                        as no_po_anywhere,
    p.po_detail

from shortfall s
left join po_by_org p
    on s.product_primary_item_nbr = p.product_primary_item_nbr

order by
    no_po_anywhere  desc,
    total_lines_short desc
