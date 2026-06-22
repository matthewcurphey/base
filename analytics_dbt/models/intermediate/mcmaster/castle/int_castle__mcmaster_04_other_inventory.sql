{{ config(materialized='table') }}

with backlog as (

    select * from {{ ref('int_castle__mcmaster_02_open_backlog') }}

),

/* =====================================================
   SHORTFALL SUMMARY
   Short lines per item + org, same as _03.
   Filter to lines_short >= 5 to avoid noise.
===================================================== */

shortfall as (

    select
        product_primary_item_nbr,
        inv_org_code                                            as short_org,
        inv_uom,
        count(*)                                                as lines_short,
        sum(comp_inv_req)                                       as total_short_qty
    from backlog
    where is_short
    group by
        product_primary_item_nbr,
        inv_org_code,
        inv_uom
    having count(*) >= 5

),

shortfall_items as (

    select distinct product_primary_item_nbr
    from shortfall

),

/* =====================================================
   OTHER ORG DEMAND
   Replicate the _01 join for orgs outside the 6-org sphere,
   scoped tightly to items that appear in the shortfall.
   comp_inv_req uses identical logic to _01.
===================================================== */

other_sales as (

    select *
    from {{ ref('int_foundation_castle__sales_salesorder') }}
    where sales_type       = 'Order'
      and lower(sales_status) = 'valid'
      and lower(line_transaction_type) like 'sales%'
      and inv_org_code not in ('ATL', 'CLE', 'DAL', 'JVL', 'LOS', 'WIE')
      and product_primary_item_nbr in (select product_primary_item_nbr from shortfall_items)

),

other_prod as (

    select *
    from {{ ref('int_foundation_castle__mfg_prodorder') }}
    where org not in ('ATL', 'CLE', 'DAL', 'JVL', 'LOS', 'WIE')

),

other_expectedusage as (

    select * from {{ ref('int_foundation_castle__mfg_expectedusage') }}

),

other_actualusage as (

    select * from {{ ref('int_foundation_castle__mfg_actualusage') }}

),

other_assembly as (

    select
        assembly_so_nbr,
        assembly_so_line,
        assembly_shipment_nbr,
        string_agg(lot_nbr, ', ' order by lot_nbr)              as assembly_lot_nbrs
    from {{ ref('int_foundation_castle__inv_inventory') }}
    where is_assembly
    group by
        assembly_so_nbr,
        assembly_so_line,
        assembly_shipment_nbr

),

other_lines as (

    select
        s.inv_org_code,
        s.product_primary_item_nbr,
        s.so_nbr,
        s.so_line,
        s.shipment_nbr,

        case
            when au.comp_issued_qty > 0             then 0
            when inv.assembly_lot_nbrs is not null  then 0
            when eu.comp_expected_qty  is not null  then eu.comp_expected_qty
            when s.product_form in ('BAR', 'STRUCTURAL', 'PLATE', 'SHEET') then
                case
                    when upper(s.ordered_uom) = 'LBS'   then s.ordered_qty
                    else                                      s.gross_weight_lbs
                end
            when s.product_form in ('TUBE', 'EXTRUSION') then
                case
                    when upper(s.ordered_uom) = 'FT'    then s.ordered_qty
                    when upper(s.ordered_uom) = 'IN'    then s.ordered_qty / 12.0
                    else                                      s.ordered_qty * s.cut_length / 12.0
                end
            else null
        end                                                     as comp_inv_req

    from other_sales s
    left join other_prod dj
        on  s.so_nbr       = dj.so_nbr
        and s.so_line      = dj.so_line
        and s.shipment_nbr = dj.so_shipment
    left join other_expectedusage eu
        on  dj.dj_nbr = eu.dj_nbr
    left join other_actualusage au
        on  dj.dj_nbr = au.dj_nbr
    left join other_assembly inv
        on  s.so_nbr       = inv.assembly_so_nbr
        and s.so_line      = inv.assembly_so_line
        and s.shipment_nbr = inv.assembly_shipment_nbr

),

other_demand as (

    select
        product_primary_item_nbr,
        inv_org_code                                            as other_org,
        sum(coalesce(comp_inv_req, 0))                          as total_demand
    from other_lines
    group by
        product_primary_item_nbr,
        inv_org_code

),

/* =====================================================
   OTHER ORG INVENTORY
   On-hand per item per other org. Excludes sub_inv NC.
===================================================== */

other_inventory as (

    select
        product_primary_item_nbr,
        inv_org_code                                            as other_org,
        sum(on_hand_units)                                      as on_hand
    from {{ ref('int_foundation_castle__inv_inventory') }}
    where inv_org_code not in ('ATL', 'CLE', 'DAL', 'JVL', 'LOS', 'WIE')
      and sub_inv_code not in ('NC')
      and product_primary_item_nbr in (select product_primary_item_nbr from shortfall_items)
    group by
        product_primary_item_nbr,
        inv_org_code

),

/* =====================================================
   OTHER ORG NET AVAILABLE
   on_hand - committed demand = genuine free balance.
   Only keep orgs where net > 0.
===================================================== */

other_net as (

    select
        i.product_primary_item_nbr,
        i.other_org,
        i.on_hand,
        coalesce(d.total_demand, 0)                             as total_demand,
        i.on_hand - coalesce(d.total_demand, 0)                 as net_available
    from other_inventory i
    left join other_demand d
        on  i.product_primary_item_nbr = d.product_primary_item_nbr
        and i.other_org                = d.other_org
    where i.on_hand - coalesce(d.total_demand, 0) > 0

),

/* =====================================================
   SHORT LINES
   Line-level detail for bin-packing — same as _03.
   Rank smallest → largest to maximise lines cleared.
===================================================== */

short_lines as (

    select
        product_primary_item_nbr,
        inv_org_code                                            as short_org,
        comp_inv_req,
        sum(comp_inv_req) over (
            partition by product_primary_item_nbr, inv_org_code
            order by comp_inv_req asc nulls last
            rows unbounded preceding
        )                                                       as cumulative_req
    from backlog
    where is_short

),

/* =====================================================
   LINES COVERABLE
   For each item + short_org + other_org:
   how many short lines fit within the other org's
   net available, working smallest first.
===================================================== */

lines_coverable as (

    select
        sl.product_primary_item_nbr,
        sl.short_org,
        n.other_org,
        count(*)                                                as lines_coverable,
        max(sl.cumulative_req)                                  as lbs_needed
    from short_lines sl
    inner join other_net n
        on  sl.product_primary_item_nbr = n.product_primary_item_nbr
    where sl.cumulative_req <= n.net_available
    group by
        sl.product_primary_item_nbr,
        sl.short_org,
        n.other_org

),

/* =====================================================
   DONOR LOAD
   Total shortfall demand pointing at each other org.
   Used to flag overcommitment across multiple short orgs.
===================================================== */

donor_load as (

    select
        n.product_primary_item_nbr,
        n.other_org,
        sum(s.total_short_qty)                                  as total_demand_on_donor
    from other_net n
    inner join shortfall s
        on n.product_primary_item_nbr = s.product_primary_item_nbr
    group by
        n.product_primary_item_nbr,
        n.other_org

)

/* =====================================================
   FINAL MATRIX
   Item × short_org × other_org grain.
   Transfer-in signal — stock moves to the short org,
   orders stay put. net_available is honest (on_hand
   minus that org's own committed demand).
===================================================== */

select
    s.product_primary_item_nbr,
    s.inv_uom,
    s.short_org,
    s.lines_short,
    s.total_short_qty,
    n.other_org,
    n.on_hand                                                   as other_org_on_hand,
    n.total_demand                                              as other_org_committed,
    n.net_available                                             as other_org_net_available,
    coalesce(lc.lines_coverable, 0)                             as lines_coverable,
    coalesce(lc.lbs_needed, 0)                                  as lbs_needed,
    round(
        coalesce(lc.lines_coverable, 0)::numeric
        / nullif(s.lines_short, 0) * 100
    , 1)                                                        as lines_coverage_pct,
    coalesce(lc.lines_coverable, 0) = s.lines_short             as full_lines_cover,
    dl.total_demand_on_donor > n.net_available                  as donor_overcommitted

from shortfall s
inner join other_net n
    on  s.product_primary_item_nbr = n.product_primary_item_nbr
left join lines_coverable lc
    on  s.product_primary_item_nbr = lc.product_primary_item_nbr
    and s.short_org                = lc.short_org
    and n.other_org                = lc.other_org
inner join donor_load dl
    on  n.product_primary_item_nbr = dl.product_primary_item_nbr
    and n.other_org                = dl.other_org

order by
    full_lines_cover        desc,
    donor_overcommitted     asc,
    lines_coverable         desc,
    lines_coverage_pct      desc
