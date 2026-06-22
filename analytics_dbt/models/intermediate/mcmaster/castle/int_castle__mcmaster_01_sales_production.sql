{{ config(materialized='table') }}

with sales as (

    select * from {{ ref('int_foundation_castle__sales_salesorder') }}

),

mcm_open_items as (

    -- open McMaster orders by org + item — used to pull competing demand from other customers
    select distinct
        inv_org_code,
        product_primary_item_nbr
    from sales
    where (ship_to_customer_nbr = '4872' or sold_to_customer_nbr = '4872')
      and sales_type = 'Order'
      and lower(sales_status) = 'valid'
      and lower(line_transaction_type) like 'sales%'

),

prodorder as (

    select * from {{ ref('int_foundation_castle__mfg_prodorder') }}

),

expectedusage as (

    select * from {{ ref('int_foundation_castle__mfg_expectedusage') }}

),

actualusage as (

    select * from {{ ref('int_foundation_castle__mfg_actualusage') }}

),

operations as (

    select
        dj_nbr,
        string_agg(operation_code, ' > ' order by operation_sequence) as operation_steps
    from {{ ref('int_foundation_castle__mfg_operations') }}
    group by dj_nbr

),

inventory as (

    select
        assembly_so_nbr,
        assembly_so_line,
        assembly_shipment_nbr,
        min(product_primary_item_nbr)                       as assembly,
        string_agg(lot_nbr, ', ' order by lot_nbr)          as assembly_lot_nbrs
    from {{ ref('int_foundation_castle__inv_inventory') }}
    where is_assembly
    group by
        assembly_so_nbr,
        assembly_so_line,
        assembly_shipment_nbr

),

raw_inventory as (

    select
        product_primary_item_nbr,
        sum(case when inv_org_code = 'ATL' then on_hand_units else 0 end)                                                       as inv_atl,
        sum(case when inv_org_code = 'CLE' then on_hand_units else 0 end)                                                       as inv_cle,
        sum(case when inv_org_code = 'DAL' then on_hand_units else 0 end)                                                       as inv_dal,
        sum(case when inv_org_code = 'JVL' then on_hand_units else 0 end)                                                       as inv_jvl,
        sum(case when inv_org_code = 'LOS' then on_hand_units else 0 end)                                                       as inv_los,
        sum(case when inv_org_code = 'WIE' then on_hand_units else 0 end)                                                       as inv_wie,
        sum(case when inv_org_code not in ('ATL', 'CLE', 'DAL', 'JVL', 'LOS', 'WIE') then on_hand_units else 0 end)            as inv_other,
        sum(on_hand_units)                                                                                                       as inv_total,
        min(uom_code)                                                                                                            as inv_uom
    from {{ ref('int_foundation_castle__inv_inventory') }}
    where sub_inv_code not in ('NC')
    group by product_primary_item_nbr

),

joined as (

    select

        /* =====================================================
           SALES COLUMNS
        ===================================================== */

        /* =======================
           S — LOCATION
           ======================= */
        s.inv_org_code,
        s.branch_name,

        /* =======================
           S — IDENTIFIERS & GRAIN
           ======================= */
        --s.company,
        s.so_nbr,
        s.so_line,
        s.shipment_nbr              as so_shipment,
        s.item_nbr,

        /* =======================
           S — STATUS
           ======================= */
        --s.sales_status,
        s.sales_type,
        --s.line_transaction_type,

        /* =======================
           S — DATES
           ======================= */
        s.order_date,
        s.promise_date,
        s.request_date,
        --s.quote_date,
        --s.actual_ship_date,
        s.invoice_date,

        /* =======================
           S — QUANTITIES
           ======================= */
        --s.ordered_lbs,
        --s.ordered_pcs,
        s.ordered_qty,
        s.ordered_uom,
        --s.invoiced_lbs,
        --s.invoiced_pcs,
        s.invoiced_qty,
        s.invoiced_uom,
        s.weight_lbs,
        s.gross_weight_lbs,

        /* =======================
           S — CUT SIZE
           ======================= */
        s.cut_shape,
        s.cut_uom,
        s.cut_width,
        s.cut_length,

        /* =======================
           S — PRODUCT ATTRIBUTES
           ======================= */
        s.product_primary_item_nbr,
        s.product_item_description,
        s.product_item_type,
        s.product_commodity,
        s.product_form,
        --s.product_grade,
        --s.product_temper,
        --s.product_shape,
        s.product_width,
        s.product_length,
        s.product_primary_dimension,
        --s.product_stocking_uom,
        
        --s.product_source_type,
        --s.product_customer,

        /* =======================
           S — FINANCIALS
           ======================= */
        s.total_sales_usd,
        --s.material_revenue,
        --s.proc_rev_usd,
        --s.freight_revenue_usd,
        --s.total_gross_profit_usd,
        --s.matl_gp_usd,
        --s.tgp_pct,
        --s.mgp_pct,
        --s.material_cost_aac,
        --s.material_overhead_cost,
        --s.outside_processing_cost,
        --s.resource_cost_usd,
        --s.absorption_cost_usd,
        --s.freight_cost,
        --s.list_price_per_lbs_gross,
        --s.price_per_lbs_gross,

        /* =======================
           S — CUSTOMER
           ======================= */
        (s.ship_to_customer_nbr = '4872' or s.sold_to_customer_nbr = '4872')   as is_mcmaster,
        --s.sold_to_customer_name,
        --s.sold_to_customer_nbr,
        s.ship_to_customer_name,
        s.ship_to_customer_nbr,

        /* =====================================================
           DJ COLUMNS
        ===================================================== */

        /* =======================
           DJ — IDENTIFIERS
           ======================= */
        dj.dj_nbr,
        dj.org                          as dj_org,

        /* =======================
           DJ — STATUS
           ======================= */
        dj.job_status,
        --dj.dj_last_updated_by,

        /* =======================
           DJ — DATES
           ======================= */
        dj.dj_start_date,
        dj.complete_date,

        /* =======================
           DJ — QUANTITIES
           ======================= */
        dj.start_qty,
        --dj.start_qty_weight,
        dj.complete_qty,
        --dj.comp_complete_lbs,
        dj.job_uom,

        /* =======================
           DJ — PRODUCT ATTRIBUTES
           ======================= */
        --dj.product_item_number          as dj_product_item_number,
        --dj.product_form                 as dj_product_form,
        --dj.product_commodity            as dj_product_commodity,
        --dj.product_grade                as dj_product_grade,
        --dj.product_shape                as dj_product_shape,
        --dj.product_primary_dimension    as dj_product_primary_dimension,
        --dj.product_condition_1,
        --dj.product_condition_2,
        --dj.product_condition_3,
        --dj.product_length               as dj_product_length,
        --dj.product_special_feature_1,
        --dj.product_special_feature_2,
        --dj.product_special_feature_3,
        --dj.product_surface,
        --dj.product_temper               as dj_product_temper,
        --dj.product_width                as dj_product_width,
        --dj.product_item_description     as dj_product_item_description,

        /* =======================
           DJ — COMPONENT & FX
           ======================= */
        dj.comp_item,
        --dj.comp_item_clean,
        dj.comp_uom,
        --dj.currency_code,
        --dj.localfx_comp_cost,
        --dj.fx_rate_to_usd,
        --dj.fx_effective_date,
        --dj.comp_cost_usd,
        --dj.wpl,
        --dj.wpl_uom,

        /* =======================
           EU — EXPECTED USAGE
           ======================= */
        eu.comp_qty_per_assy,
        eu.comp_expected_qty,
        --eu.comp_expected_lbs,

        /* =======================
           AU — ACTUAL USAGE
           ======================= */
        au.comp_issued_qty,
        --au.comp_issued_usd,
        --au.comp_issued_lbs,

        /* =======================
           OPS — OPERATIONS
           ======================= */
        ops.operation_steps,

        /* =======================
           INV — WIP INVENTORY
           Null when no assembly inventory on hand; presence = material issued to job
           ======================= */
        inv.assembly,
        inv.assembly_lot_nbrs,

        /* =======================
           RI — RAW ON HAND BY ORG
           Joined on comp_item; units only; excludes sub_inv NC
           ======================= */
        coalesce(ri.inv_atl,   0)   as inv_atl,
        coalesce(ri.inv_cle,   0)   as inv_cle,
        coalesce(ri.inv_dal,   0)   as inv_dal,
        coalesce(ri.inv_jvl,   0)   as inv_jvl,
        coalesce(ri.inv_los,   0)   as inv_los,
        coalesce(ri.inv_wie,   0)   as inv_wie,
        coalesce(ri.inv_other, 0)           as inv_other,
        coalesce(ri.inv_total, 0)           as inv_total,
        ri.product_primary_item_nbr         as inv_item,
        ri.inv_uom,

        /* =======================
           DERIVED — INVENTORY REQUIREMENT
           Priority: issued → assembled → comp_expected → form-based derivation
           comp_inv_req_uom uses comp_uom for make items (comp_expected path),
           form rules for PPS/oddballs, null when zero or unresolvable
           ======================= */
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
        end                                         as comp_inv_req,

        case
            when au.comp_issued_qty > 0             then null
            when inv.assembly_lot_nbrs is not null  then null
            when eu.comp_expected_qty  is not null  then dj.comp_uom
            when s.product_form in ('BAR', 'STRUCTURAL', 'PLATE', 'SHEET')   then 'LBS'
            when s.product_form in ('TUBE', 'EXTRUSION')            then 'FT'
            else null
        end                                         as comp_inv_req_uom

    from sales s
    full outer join prodorder dj
        on  s.so_nbr        = dj.so_nbr
        and s.so_line       = dj.so_line
        and s.shipment_nbr  = dj.so_shipment
    left join expectedusage eu
        on  dj.dj_nbr = eu.dj_nbr
    left join actualusage au
        on  dj.dj_nbr = au.dj_nbr
    left join operations ops
        on  dj.dj_nbr = ops.dj_nbr
    left join inventory inv
        on  s.so_nbr       = inv.assembly_so_nbr
        and s.so_line      = inv.assembly_so_line
        and s.shipment_nbr = inv.assembly_shipment_nbr
    left join raw_inventory ri
        on  s.product_primary_item_nbr = ri.product_primary_item_nbr

    where
        lower(s.sales_status) = 'valid'
        and lower(s.line_transaction_type) like 'sales%'
        and (
            -- McMaster: all valid sales lines
            (s.ship_to_customer_nbr = '4872' or s.sold_to_customer_nbr = '4872')
            -- Other customers: open orders competing for the same org + item as an open McMaster order
            or (
                s.sales_type = 'Order'
                and (s.inv_org_code, s.product_primary_item_nbr) in (
                    select inv_org_code, product_primary_item_nbr from mcm_open_items
                )
            )
        )

)

select *
from joined
