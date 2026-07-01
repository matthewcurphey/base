{{ config(materialized='table') }}

with src as (

    -- Compute item_clean once here so both SELECT and JOIN can use it.
    select
        *,
        regexp_replace(
            item_number,
            '(_1(-P)?(\.(BO|CO|MO|MC))?|-P(\.(BO|CO|MO|MC))?|\.(BO|CO|MO|MC))$',
            ''
        ) as item_clean
    from {{ ref('stg_castle_oracle__open_orders') }}

),

di as (

    -- Product attributes not in the order report (form, commodity, UOM, make/buy).
    -- One row per item; attributes are consistent across orgs so min() is safe.
    select
        item_number,
        min(product_form)       as product_form,
        min(commodity)          as commodity,
        min(unit_of_measure)    as unit_of_measure,
        min(make_buy)           as make_buy
    from {{ ref('stg_castle_oracle__di') }}
    group by item_number

),

joined as (

    select

        /* =======================
           LOCATION
           ======================= */
        src.shipping_org                                                as inv_org_code,
        src.branch_name,

        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        src.so_nbr,
        src.so_line,
        src.so_shipment,
        src.item_number                                                 as item_nbr,
        src.item_clean,
        src.order_line,

        /* =======================
           STATUS
           ======================= */
        src.order_status,
        src.pick_status,
        src.credit_hold,

        /* =======================
           DATES
           ======================= */
        src.ordered_date                                                as order_date,
        src.request_date,
        src.promise_date,
        src.scheduled_ship_date,
        src.actual_ship_date                                            as invoice_date,
        src.transaction_date,

        /* =======================
           QUANTITIES
           ======================= */
        src.ordered_qty,
        src.ordered_uom,
        src.lbs_qty                                                     as weight_lbs,
        src.cut_width,
        src.cut_length,
        src.required_qty                                                as comp_expected_qty,
        src.quantity_issued                                             as comp_issued_qty,
        src.quantity_on_hand,

        /* =======================
           PRODUCT ATTRIBUTES
           From stg: grade, temper, dimensions
           From DI join: form, commodity, UOM, sourcing type
           ======================= */
        src.ordered_item_desc                                           as product_item_description,
        src.item_grade                                                  as product_grade,
        src.item_temper                                                 as product_temper,
        src.item_thickness,
        di.product_form,
        di.commodity                                                    as product_commodity,
        di.unit_of_measure,
        di.make_buy,

        /* =======================
           FINANCIALS
           ======================= */
        src.sales_usd                                                   as total_sales_usd,
        src.cogs,
        src.gross_margin,
        src.margin_pct,

        /* =======================
           CUSTOMER
           ======================= */
        src.customer_name                                               as ship_to_customer_name,
        src.customer_po_number,
        src.cust_item_number,

        /* =======================
           DJ
           ======================= */
        src.discrete_job                                                as dj_nbr,
        src.discrete_job_status                                         as job_status,
        src.task_status,

        /* =======================
           PERFORMANCE
           ======================= */
        src.days_early_late,
        src.on_time_pct,
        src.sales_representative

    from src
    left join di on src.item_clean = di.item_number

)

select * from joined
