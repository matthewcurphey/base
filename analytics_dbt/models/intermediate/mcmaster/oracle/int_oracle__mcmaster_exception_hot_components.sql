{{ config(materialized='table') }}

with backlog as (

    select * from {{ ref('int_oracle__mcmaster_02_open_backlog') }}

),

po as (

    -- Castle PO system is still the source of truth for open POs even for
    -- oracle-side orgs — same shared item master, so item_clean joins
    -- directly to product_primary_item_nbr.
    select * from {{ ref('int_foundation_castle__po_open') }}

),

/* =====================================================
   SHORTFALL BY ITEM
   One row per component with total + per-org short line
   counts. Only items with at least one short line —
   sorting is left to the mart/output layer.
===================================================== */

shortfall as (

    select
        item_clean,
        max(unit_of_measure)                                                as uom,

        sum(case when inv_org_code = 'ATL' and is_short then 1 else 0 end) as lines_short_atl,
        sum(case when inv_org_code = 'CLE' and is_short then 1 else 0 end) as lines_short_cle,
        sum(case when inv_org_code = 'DAL' and is_short then 1 else 0 end) as lines_short_dal,
        sum(case when inv_org_code = 'JVL' and is_short then 1 else 0 end) as lines_short_jvl,
        sum(case when inv_org_code = 'LOS' and is_short then 1 else 0 end) as lines_short_los,
        sum(case when inv_org_code = 'WIE' and is_short then 1 else 0 end) as lines_short_wie,

        sum(case when is_short then 1          else 0 end)                as total_lines_short,
        sum(case when is_short then comp_inv_req else 0 end)               as total_qty_short

    from backlog
    where is_short
    group by item_clean

),

shortfall_items as (

    select distinct item_clean from shortfall

),

/* =====================================================
   OPEN PO DETAIL
   One JSON array per item — org, mill, due date, qty —
   for every open PO line against that item, company-wide.
===================================================== */

po_detail as (

    select
        product_primary_item_nbr                                           as item_clean,
        jsonb_agg(
            jsonb_build_object(
                'org',  inv_org_code,
                'mill', vendor_name,
                'date', to_char(po_due_date, 'YYYY-MM-DD'),
                'qty',  po_open_qty
            )
            order by po_due_date asc nulls last
        )                                                                   as po_details
    from po
    where product_primary_item_nbr in (select item_clean from shortfall_items)
    group by product_primary_item_nbr

),

/* =====================================================
   OTHER ORG DEMAND & INVENTORY
   These orgs never moved to Oracle, so castle remains the
   only source for both their sales and inventory, regardless
   of where McMaster's own orders live. Same comp_inv_req
   derivation logic as the castle foundation models. Sphere
   orgs (ATL/CLE/DAL/JVL/LOS/WIE) are excluded — that's
   cross_ship's job.
===================================================== */

other_sales as (

    select *
    from {{ ref('int_foundation_castle__sales_salesorder') }}
    where sales_type          = 'Order'
      and lower(sales_status) = 'valid'
      and lower(line_transaction_type) like 'sales%'
      and inv_org_code not in ('ATL', 'CLE', 'DAL', 'JVL', 'LOS', 'WIE')
      and product_primary_item_nbr in (select item_clean from shortfall_items)

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
        s.product_primary_item_nbr                              as item_clean,

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
        item_clean,
        inv_org_code                                            as other_org,
        sum(coalesce(comp_inv_req, 0))                          as total_demand
    from other_lines
    group by
        item_clean,
        inv_org_code

),

other_inventory as (

    select
        product_primary_item_nbr                                as item_clean,
        inv_org_code                                             as other_org,
        sum(on_hand_units)                                       as on_hand
    from {{ ref('int_foundation_castle__inv_inventory') }}
    where inv_org_code not in ('ATL', 'CLE', 'DAL', 'JVL', 'LOS', 'WIE')
      and sub_inv_code not in ('NC')
      and product_primary_item_nbr in (select item_clean from shortfall_items)
    group by
        product_primary_item_nbr,
        inv_org_code

),

/* =====================================================
   OTHER ORG NET AVAILABLE
   on_hand - committed demand = genuine free balance.
   Only positive net counts as "available" — matches
   castle _04. Pivoted one column per org.
===================================================== */

other_net as (

    select
        i.item_clean,
        i.other_org,
        i.on_hand - coalesce(d.total_demand, 0)                 as net_available
    from other_inventory i
    left join other_demand d
        on  i.item_clean = d.item_clean
        and i.other_org  = d.other_org
    where i.on_hand - coalesce(d.total_demand, 0) > 0

),

other_net_pivot as (

    select
        item_clean,
        coalesce(sum(case when other_org = 'ENT' then net_available end), 0) as avail_ent,
        coalesce(sum(case when other_org = 'ENA' then net_available end), 0) as avail_ena,
        coalesce(sum(case when other_org = 'AAA' then net_available end), 0) as avail_aaa,
        coalesce(sum(case when other_org = 'MXM' then net_available end), 0) as avail_mxm,
        coalesce(sum(case when other_org = 'TOR' then net_available end), 0) as avail_tor,
        coalesce(sum(case when other_org = 'ASC' then net_available end), 0) as avail_asc,
        coalesce(sum(case when other_org = 'MTY' then net_available end), 0) as avail_mty,
        coalesce(sum(case when other_org = 'MXQ' then net_available end), 0) as avail_mxq,
        coalesce(sum(case when other_org = 'SGP' then net_available end), 0) as avail_sgp,
        coalesce(sum(case when other_org = 'HAI' then net_available end), 0) as avail_hai,
        coalesce(sum(case when other_org = 'MCH' then net_available end), 0) as avail_mch,
        coalesce(sum(case when other_org = 'CON' then net_available end), 0) as avail_con,
        coalesce(sum(case when other_org = 'CHA' then net_available end), 0) as avail_cha,
        coalesce(sum(case when other_org = 'STO' then net_available end), 0) as avail_sto,
        coalesce(sum(case when other_org = 'WIN' then net_available end), 0) as avail_win,
        coalesce(sum(case when other_org = 'ADC' then net_available end), 0) as avail_adc
    from other_net
    group by item_clean

)

select
    s.item_clean,
    s.uom,
    s.total_lines_short,
    s.total_qty_short,
    s.lines_short_atl,
    s.lines_short_cle,
    s.lines_short_dal,
    s.lines_short_jvl,
    s.lines_short_los,
    s.lines_short_wie,
    p.po_details,
    coalesce(onp.avail_ent, 0)                                  as avail_ent,
    coalesce(onp.avail_ena, 0)                                  as avail_ena,
    coalesce(onp.avail_aaa, 0)                                  as avail_aaa,
    coalesce(onp.avail_mxm, 0)                                  as avail_mxm,
    coalesce(onp.avail_tor, 0)                                  as avail_tor,
    coalesce(onp.avail_asc, 0)                                  as avail_asc,
    coalesce(onp.avail_mty, 0)                                  as avail_mty,
    coalesce(onp.avail_mxq, 0)                                  as avail_mxq,
    coalesce(onp.avail_sgp, 0)                                  as avail_sgp,
    coalesce(onp.avail_hai, 0)                                  as avail_hai,
    coalesce(onp.avail_mch, 0)                                  as avail_mch,
    coalesce(onp.avail_con, 0)                                  as avail_con,
    coalesce(onp.avail_cha, 0)                                  as avail_cha,
    coalesce(onp.avail_sto, 0)                                  as avail_sto,
    coalesce(onp.avail_win, 0)                                  as avail_win,
    coalesce(onp.avail_adc, 0)                                  as avail_adc

from shortfall s
left join po_detail p
    on s.item_clean = p.item_clean
left join other_net_pivot onp
    on s.item_clean = onp.item_clean
