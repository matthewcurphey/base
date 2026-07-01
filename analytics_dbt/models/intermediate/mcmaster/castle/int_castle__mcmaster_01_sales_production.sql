{{ config(materialized='table') }}

with base as (

    select * from {{ ref('int_foundation_castle__sales_production') }}
    where lower(sales_status) = 'valid'
      and lower(line_transaction_type) like 'sales%'

),

mcm_open_items as (

    -- open McMaster orders by org + item — used to pull competing demand from other customers
    select distinct
        inv_org_code,
        product_primary_item_nbr
    from base
    where (ship_to_customer_nbr = '4872' or sold_to_customer_nbr = '4872')
      and sales_type = 'Order'

),

mcm_scope as (

    select base.*
    from base
    left join mcm_open_items mcm
        on  base.inv_org_code             = mcm.inv_org_code
        and base.product_primary_item_nbr = mcm.product_primary_item_nbr
    where
        (
            (base.ship_to_customer_nbr = '4872' or base.sold_to_customer_nbr = '4872')
            and base.so_shipment::int <= 1
        )
        or (
            base.sales_type = 'Order'
            and mcm.inv_org_code is not null
        )

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
        base.inv_org_code,
        base.branch_name,

        /* =======================
           S — IDENTIFIERS & GRAIN
           ======================= */
        base.so_nbr,
        base.so_line,
        base.so_shipment,
        base.item_nbr,

        /* =======================
           S — STATUS
           ======================= */
        base.sales_type,

        /* =======================
           S — DATES
           ======================= */
        base.order_date,
        base.promise_date,
        base.request_date,
        base.invoice_date,

        /* =======================
           S — QUANTITIES
           ======================= */
        base.ordered_qty,
        base.ordered_uom,
        base.invoiced_qty,
        base.invoiced_uom,
        base.weight_lbs,
        base.gross_weight_lbs,

        /* =======================
           S — CUT SIZE
           ======================= */
        base.cut_shape,
        base.cut_uom,
        base.cut_width,
        base.cut_length,

        /* =======================
           S — PRODUCT ATTRIBUTES
           ======================= */
        base.product_primary_item_nbr,
        base.product_item_description,
        base.product_item_type,
        base.product_commodity,
        base.product_form,
        base.product_width,
        base.product_length,
        base.product_primary_dimension,

        /* =======================
           S — FINANCIALS
           ======================= */
        base.total_sales_usd,

        /* =======================
           S — CUSTOMER
           ======================= */
        (base.ship_to_customer_nbr = '4872' or base.sold_to_customer_nbr = '4872')   as is_mcmaster,
        base.ship_to_customer_name,
        base.ship_to_customer_nbr,

        /* =====================================================
           DJ COLUMNS
        ===================================================== */

        /* =======================
           DJ — IDENTIFIERS
           ======================= */
        base.dj_nbr,
        base.dj_org,

        /* =======================
           DJ — STATUS
           ======================= */
        base.job_status,

        /* =======================
           DJ — DATES
           ======================= */
        base.dj_start_date,
        base.complete_date,

        /* =======================
           DJ — QUANTITIES
           ======================= */
        base.start_qty,
        base.complete_qty,
        base.job_uom,

        /* =======================
           DJ — COMPONENT & FX
           ======================= */
        base.comp_item,
        base.comp_uom,

        /* =======================
           EU — EXPECTED USAGE
           ======================= */
        base.comp_qty_per_assy,
        base.comp_expected_qty,

        /* =======================
           AU — ACTUAL USAGE
           ======================= */
        base.comp_issued_qty,

        /* =======================
           OPS — OPERATIONS
           ======================= */
        base.operation_steps,

        /* =======================
           INV — WIP INVENTORY
           Null when no assembly inventory on hand; presence = material issued to job
           ======================= */
        inv.assembly,
        inv.assembly_lot_nbrs,

        /* =======================
           RI — RAW ON HAND BY ORG
           Joined on product_primary_item_nbr; units only; excludes sub_inv NC
           ======================= */
        coalesce(ri.inv_atl,   0)           as inv_atl,
        coalesce(ri.inv_cle,   0)           as inv_cle,
        coalesce(ri.inv_dal,   0)           as inv_dal,
        coalesce(ri.inv_jvl,   0)           as inv_jvl,
        coalesce(ri.inv_los,   0)           as inv_los,
        coalesce(ri.inv_wie,   0)           as inv_wie,
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
            when base.comp_issued_qty > 0           then 0
            when inv.assembly_lot_nbrs is not null  then 0
            when base.comp_expected_qty is not null then base.comp_expected_qty
            when base.product_form in ('BAR', 'STRUCTURAL', 'PLATE', 'SHEET') then
                case
                    when upper(base.ordered_uom) = 'LBS'   then base.ordered_qty
                    else                                         base.gross_weight_lbs
                end
            when base.product_form in ('TUBE', 'EXTRUSION') then
                case
                    when upper(base.ordered_uom) = 'FT'    then base.ordered_qty
                    when upper(base.ordered_uom) = 'IN'    then base.ordered_qty / 12.0
                    else                                         base.ordered_qty * base.cut_length / 12.0
                end
            else null
        end                                         as comp_inv_req,

        case
            when base.comp_issued_qty > 0           then null
            when inv.assembly_lot_nbrs is not null  then null
            when base.comp_expected_qty is not null then base.comp_uom
            when base.product_form in ('BAR', 'STRUCTURAL', 'PLATE', 'SHEET')  then 'LBS'
            when base.product_form in ('TUBE', 'EXTRUSION')                    then 'FT'
            else null
        end                                         as comp_inv_req_uom

    from mcm_scope as base
    left join inventory inv
        on  base.so_nbr      = inv.assembly_so_nbr
        and base.so_line     = inv.assembly_so_line
        and base.so_shipment = inv.assembly_shipment_nbr
    left join raw_inventory ri
        on  base.product_primary_item_nbr = ri.product_primary_item_nbr

)

select *
from joined
