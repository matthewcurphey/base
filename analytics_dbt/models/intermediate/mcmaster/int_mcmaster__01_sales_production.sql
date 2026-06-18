{{ config(materialized='view') }}

with sales as (

    select * from {{ ref('int_foundation_castle__sales_salesorder') }}

),

prodorder as (

    select * from {{ ref('int_foundation_castle__mfg_prodorder') }}

),

joined as (

    select

        /* =====================================================
           SALES COLUMNS
        ===================================================== */

        /* =======================
           S — IDENTIFIERS & GRAIN
           ======================= */
        s.company,
        s.so_nbr,
        s.so_line,
        s.shipment_nbr              as so_shipment,
        s.item_nbr,

        /* =======================
           S — LOCATION
           ======================= */
        s.inv_org_code,
        s.branch_name,

        /* =======================
           S — STATUS
           ======================= */
        s.sales_status,
        s.sales_type,
        s.line_transaction_type,

        /* =======================
           S — DATES
           ======================= */
        s.order_date,
        s.promise_date,
        s.request_date,
        s.quote_date,
        s.actual_ship_date,

        /* =======================
           S — QUANTITIES
           ======================= */
        s.ordered_lbs,
        s.ordered_pcs,
        s.ordered_qty,
        s.ordered_uom,
        s.weight_lbs,
        s.gross_weight_lbs,

        /* =======================
           S — CUT SIZE
           ======================= */
        s.cut_uom,
        s.cut_shape,
        s.cut_width,
        s.cut_length,

        /* =======================
           S — PRODUCT ATTRIBUTES
           ======================= */
        s.product_primary_item_nbr,
        s.product_item_description,
        s.product_item_type,
        s.product_form,
        s.product_grade,
        s.product_shape,
        s.product_temper,
        s.product_width,
        s.product_length,
        s.product_primary_dimension,
        s.product_stocking_uom,
        s.product_commodity,
        s.product_source_type,

        /* =======================
           S — CUSTOMER
           ======================= */
        s.sold_to_customer_name,
        s.sold_to_customer_nbr,
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
        dj.dj_last_updated_by,

        /* =======================
           DJ — DATES
           ======================= */
        dj.dj_start_date,
        dj.complete_date,

        /* =======================
           DJ — QUANTITIES
           ======================= */
        dj.start_qty,
        dj.start_qty_weight,
        dj.complete_qty,
        dj.comp_complete_lbs,
        dj.job_uom,

        /* =======================
           DJ — PRODUCT ATTRIBUTES
           ======================= */
        dj.product_item_number          as dj_product_item_number,
        dj.product_form                 as dj_product_form,
        dj.product_commodity            as dj_product_commodity,
        dj.product_grade                as dj_product_grade,
        dj.product_shape                as dj_product_shape,
        dj.product_primary_dimension    as dj_product_primary_dimension,
        dj.product_condition_1,
        dj.product_condition_2,
        dj.product_condition_3,
        dj.product_length               as dj_product_length,
        dj.product_special_feature_1,
        dj.product_special_feature_2,
        dj.product_special_feature_3,
        dj.product_surface,
        dj.product_temper               as dj_product_temper,
        dj.product_width                as dj_product_width,
        dj.product_item_description     as dj_product_item_description,

        /* =======================
           DJ — COMPONENT & FX
           ======================= */
        dj.comp_item,
        dj.comp_item_clean,
        dj.comp_uom,
        dj.currency_code,
        dj.localfx_comp_cost,
        dj.fx_rate_to_usd,
        dj.fx_effective_date,
        dj.comp_cost_usd,
        dj.wpl,
        dj.wpl_uom

    from sales s
    full outer join prodorder dj
        on  s.so_nbr        = dj.so_nbr
        and s.so_line       = dj.so_line
        and s.shipment_nbr  = dj.so_shipment

)

select *
from joined
