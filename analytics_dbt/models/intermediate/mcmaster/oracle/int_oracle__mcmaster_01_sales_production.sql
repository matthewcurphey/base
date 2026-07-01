{{ config(materialized='table') }}

with base as (

    select * from {{ ref('int_foundation_oracle__sales_production') }}

),

mcm_open_items as (

    -- Items McMaster has open orders for, by org — used to scope competing demand.
    select distinct
        inv_org_code,
        item_clean
    from base
    where ship_to_customer_name ilike '%mc%master%'

),

mcm_scope as (

    -- McMaster open orders + other customers' open demand for the same item+org.
    -- Competing demand is included so the tally in model 02 deducts it from inventory.
    select base.*
    from base
    left join mcm_open_items mcm
        on  base.inv_org_code = mcm.inv_org_code
        and base.item_clean   = mcm.item_clean
    where
        base.ship_to_customer_name ilike '%mc%master%'
        or mcm.inv_org_code is not null

),

assembly as (

    -- WIP material already cut for a specific order — lot numbers confirm material staged.
    select
        assembly_so_nbr,
        assembly_so_line,
        assembly_shipment_nbr,
        min(item)                                               as assembly,
        string_agg(lot_nbr, ', ' order by lot_nbr)             as assembly_lot_nbrs
    from {{ ref('int_foundation_oracle__inv_inventory') }}
    where is_assembly
    group by assembly_so_nbr, assembly_so_line, assembly_shipment_nbr

),

raw_inventory as (

    -- Oracle inventory aggregated to item level in native UOM, pivoted by org.
    -- Excludes NC (non-conforming) sub-inventory.
    select
        item_clean,
        sum(case when org = 'ATL' then coalesce(on_hand_uom_qty, 0) else 0 end)                                                        as inv_atl,
        sum(case when org = 'CLE' then coalesce(on_hand_uom_qty, 0) else 0 end)                                                        as inv_cle,
        sum(case when org = 'DAL' then coalesce(on_hand_uom_qty, 0) else 0 end)                                                        as inv_dal,
        sum(case when org = 'JVL' then coalesce(on_hand_uom_qty, 0) else 0 end)                                                        as inv_jvl,
        sum(case when org = 'LOS' then coalesce(on_hand_uom_qty, 0) else 0 end)                                                        as inv_los,
        sum(case when org = 'WIE' then coalesce(on_hand_uom_qty, 0) else 0 end)                                                        as inv_wie
    from {{ ref('int_foundation_oracle__inv_inventory') }}
    where sub_inv not in ('NC')
    group by item_clean

),

cx as (

    -- CX team's reference annotations for open order lines (SharePoint).
    -- Uses the most recent snapshot tab; max() collapses any within-date dupes.
    select
        trim(order_line)        as order_line,
        max(trim(reason))       as cx_reason,
        max(trim(vendor))       as cx_vendor,
        max(trim(po))           as cx_po,
        max(trim(comments))     as cx_comments
    from {{ source('oracle', 'cx_orders') }}
    where snapshot_date = (
        select max(snapshot_date)
        from {{ source('oracle', 'cx_orders') }}
    )
    group by trim(order_line)

),

joined as (

    select

        /* =======================
           LOCATION
           ======================= */
        base.inv_org_code,
        base.branch_name,

        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        base.so_nbr,
        base.so_line,
        base.so_shipment,
        base.item_nbr,
        base.item_clean,

        /* =======================
           STATUS
           ======================= */
        base.order_status,
        base.pick_status,
        base.credit_hold,

        /* =======================
           DATES
           ======================= */
        base.order_date,
        base.promise_date,
        base.request_date,
        base.scheduled_ship_date,
        base.invoice_date,

        /* =======================
           QUANTITIES
           ======================= */
        base.ordered_qty,
        base.ordered_uom,
        base.weight_lbs,
        base.cut_width,
        base.cut_length,

        /* =======================
           PRODUCT ATTRIBUTES
           ======================= */
        base.product_item_description,
        base.product_grade,
        base.product_temper,
        base.item_thickness,
        base.product_form,
        base.product_commodity,
        base.unit_of_measure,
        base.make_buy,

        /* =======================
           FINANCIALS
           ======================= */
        base.total_sales_usd,
        base.cogs,
        base.gross_margin,
        base.margin_pct,

        /* =======================
           CUSTOMER
           ======================= */
        (base.ship_to_customer_name ilike '%mc%master%')        as is_mcmaster,
        base.ship_to_customer_name,
        base.customer_po_number,
        base.cust_item_number,

        /* =======================
           DJ
           ======================= */
        base.dj_nbr,
        base.job_status,
        base.task_status,

        /* =======================
           COMPONENT QUANTITIES
           ======================= */
        base.comp_expected_qty,
        base.comp_issued_qty,
        base.quantity_on_hand,

        /* =======================
           PERFORMANCE
           ======================= */
        base.days_early_late,
        base.on_time_pct,
        base.sales_representative,

        /* =======================
           WIP ASSEMBLY INVENTORY
           Null when no assembly material staged for this order line.
           ======================= */
        inv_asm.assembly,
        inv_asm.assembly_lot_nbrs,

        /* =======================
           RAW INVENTORY BY ORG
           Joined on item_clean; native UOM from Oracle inventory foundation.
           ======================= */
        coalesce(ri.inv_atl,   0)                               as inv_atl,
        coalesce(ri.inv_cle,   0)                               as inv_cle,
        coalesce(ri.inv_dal,   0)                               as inv_dal,
        coalesce(ri.inv_jvl,   0)                               as inv_jvl,
        coalesce(ri.inv_los,   0)                               as inv_los,
        coalesce(ri.inv_wie,   0)                               as inv_wie,

        /* =======================
           CX REFERENCE
           Annotations from the CX team's SharePoint open orders file.
           ======================= */
        cx.cx_reason,
        cx.cx_vendor,
        cx.cx_po,
        cx.cx_comments,

        /* =======================
           DERIVED — INVENTORY REQUIREMENT
           Priority: issued → WIP staged → comp_expected → form-based derivation.
           Form-based fires for direct-ship lines (no DJ) to ensure inventory is deducted in the tally.
           ======================= */
        case
            when base.comp_issued_qty > 0               then 0
            when inv_asm.assembly_lot_nbrs is not null  then 0
            when base.comp_expected_qty is not null     then base.comp_expected_qty
            when base.product_form in ('BAR', 'STRUCTURAL', 'PLATE', 'SHEET') then
                case
                    when upper(base.ordered_uom) = 'LBS'   then base.ordered_qty
                    else                                         base.weight_lbs
                end
            when base.product_form in ('TUBE', 'EXTRUSION') then
                case
                    when upper(base.ordered_uom) = 'FT'    then base.ordered_qty
                    when upper(base.ordered_uom) = 'IN'    then base.ordered_qty / 12.0
                    else                                         base.ordered_qty * base.cut_length / 12.0
                end
            else null
        end                                                     as comp_inv_req

    from mcm_scope as base
    left join assembly inv_asm
        on  base.so_nbr      = inv_asm.assembly_so_nbr
        and base.so_line     = inv_asm.assembly_so_line
        and base.so_shipment = inv_asm.assembly_shipment_nbr
    left join raw_inventory ri on base.item_clean = ri.item_clean
    left join cx on base.order_line = cx.order_line

)

select * from joined
