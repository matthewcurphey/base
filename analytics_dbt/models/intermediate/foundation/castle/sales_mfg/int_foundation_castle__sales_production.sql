{{ config(materialized='table') }}

with sales as (

    select * from {{ ref('int_foundation_castle__sales_salesorder') }}

),

prodorder as (

    select * from {{ ref('int_foundation_castle__mfg_prodorder') }}

),

prodorder_linked as (

    /* =====================================================
       Resolve SO/line/shipment for DJs that have no direct
       link. When so_nbr is null and comp_item is an assembly
       item (contains * and has a 4th segment after the 3rd *),
       parse the first three segments as so_nbr / so_line /
       so_shipment. FG DJs and others with no * are left null
       and will not join to a sales row.
    ===================================================== */

    select
        *,

        case
            when so_nbr is not null                                     then 'default'
            when position('*' in product_item_number) > 0
             and split_part(product_item_number, '*', 3) != ''          then 'assembly'
        end                                                             as dj_link_source,

        coalesce(
            so_nbr,
            case
                when position('*' in product_item_number) > 0
                 and split_part(product_item_number, '*', 3) != ''
                then split_part(product_item_number, '*', 1)
            end
        )                                                               as so_nbr_linked,

        coalesce(
            so_line,
            case
                when position('*' in product_item_number) > 0
                 and split_part(product_item_number, '*', 3) != ''
                then split_part(product_item_number, '*', 2)
            end
        )                                                               as so_line_linked,

        coalesce(
            so_shipment,
            case
                when position('*' in product_item_number) > 0
                 and split_part(product_item_number, '*', 3) != ''
                then split_part(product_item_number, '*', 3)
            end
        )                                                               as so_shipment_linked

    from prodorder

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

)

/* =====================================================
   Unfiltered full outer join of every sales order line
   to every discrete job, enriched with DJ-level expected
   usage, actual usage, and operation steps. No business
   logic or customer filters — downstream models apply those.
===================================================== */

select

    /* =======================
       S — LOCATION
       ======================= */
    s.inv_org_code,
    s.branch_name,

    /* =======================
       S — IDENTIFIERS & GRAIN
       ======================= */
    s.so_nbr,
    s.so_line,
    s.shipment_nbr              as so_shipment,
    s.item_nbr,

    /* =======================
       S — STATUS (kept for downstream filtering)
       ======================= */
    s.sales_type,
    s.sales_status,
    s.line_transaction_type,

    /* =======================
       S — DATES
       ======================= */
    s.order_date,
    s.promise_date,
    s.request_date,
    s.invoice_date,

    /* =======================
       S — QUANTITIES
       ======================= */
    s.ordered_qty,
    s.ordered_uom,
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
    s.product_width,
    s.product_length,
    s.product_primary_dimension,

    /* =======================
       S — FINANCIALS
       ======================= */
    s.total_sales_usd,

    /* =======================
       S — CUSTOMER (kept for downstream filtering)
       ======================= */
    s.ship_to_customer_name,
    s.ship_to_customer_nbr,
    s.sold_to_customer_nbr,
    s.sold_to_customer_name,

    /* =======================
       DJ — IDENTIFIERS
       ======================= */
    dj.dj_nbr,
    dj.org                      as dj_org,
    dj.dj_link_source,

    /* =======================
       DJ — STATUS
       ======================= */
    dj.job_status,

    /* =======================
       DJ — DATES
       ======================= */
    dj.dj_start_date,
    dj.complete_date,

    /* =======================
       DJ — QUANTITIES
       ======================= */
    dj.start_qty,
    dj.complete_qty,
    dj.job_uom,

    /* =======================
       DJ — COMPONENT
       ======================= */
    dj.comp_item,
    dj.comp_uom,

    /* =======================
       EU — EXPECTED USAGE
       ======================= */
    eu.comp_qty_per_assy,
    eu.comp_expected_qty,

    /* =======================
       AU — ACTUAL USAGE
       ======================= */
    au.comp_issued_qty,

    /* =======================
       OPS — OPERATIONS
       ======================= */
    ops.operation_steps

from sales s
left join prodorder_linked dj
    on  s.so_nbr       = dj.so_nbr_linked
    and s.so_line      = dj.so_line_linked
    and s.shipment_nbr = dj.so_shipment_linked
left join expectedusage eu
    on  dj.dj_nbr = eu.dj_nbr
left join actualusage au
    on  dj.dj_nbr = au.dj_nbr
left join operations ops
    on  dj.dj_nbr = ops.dj_nbr
