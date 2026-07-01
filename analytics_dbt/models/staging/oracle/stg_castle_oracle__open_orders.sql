{{ config(materialized='view') }}

with base as (
    select * from {{ ref('base_castle_oracle__open_orders') }}
),

staged as (

    select

        /* =======================
           LOCATION
           ======================= */
        branch_number::text                                             as branch_number,
        branch_name::text                                               as branch_name,
        shipping_org::text                                              as shipping_org,

        /* =======================
           ORDER IDENTIFIERS
           Order #-Line# format is SO-LINE.SHIPMENT (e.g. 7323554-4.1).
           Discrete Job comes in as float-string (e.g. '1234567.0') due to nulls;
           strip the trailing .0 before casting to text.
           ======================= */
        split_part(order_line, '-', 1)::text                           as so_nbr,
        split_part(split_part(order_line, '-', 2), '.', 1)::text       as so_line,
        split_part(order_line, '.', 2)::text                           as so_shipment,
        order_line::text                                                as order_line,
        regexp_replace(discrete_job, '\.0$', '')::text                 as discrete_job,
        discrete_job_status::text                                       as discrete_job_status,
        regexp_replace(task_status, '\.0$', '')::text                  as task_status,

        /* =======================
           CUSTOMER
           ======================= */
        customer_name::text                                             as customer_name,
        customer_po_number::text                                        as customer_po_number,

        /* =======================
           PRODUCT
           ======================= */
        cust_item_number::text                                          as cust_item_number,
        item_number::text                                               as item_number,
        ordered_item_desc::text                                         as ordered_item_desc,
        item_grade::text                                                as item_grade,
        item_temper::text                                               as item_temper,
        item_thickness::text                                            as item_thickness,

        /* =======================
           QUANTITIES
           ======================= */
        nullif(ordered_qty, '')::numeric(18,6)                         as ordered_qty,
        ordered_uom::text                                               as ordered_uom,
        nullif(lbs_qty, '')::numeric(18,6)                             as lbs_qty,
        nullif(cut_width, '')::numeric(18,6)                           as cut_width,
        nullif(cut_length, '')::numeric(18,6)                          as cut_length,
        nullif(required_qty, '')::numeric(18,6)                        as required_qty,
        nullif(quantity_issued, '')::numeric(18,6)                     as quantity_issued,
        nullif(quantity_on_hand, '')::numeric(18,6)                    as quantity_on_hand,

        /* =======================
           DATES
           Raw dates arrive as DD-Mon-YY (e.g. 18-JUN-26).
           Inject century prefix before parsing so YY becomes YYYY.
           ======================= */
        to_date(regexp_replace(nullif(ordered_date, ''),        '(\d{2})$', '20\1'), 'DD-Mon-YYYY')   as ordered_date,
        to_date(regexp_replace(nullif(request_date, ''),        '(\d{2})$', '20\1'), 'DD-Mon-YYYY')   as request_date,
        to_date(regexp_replace(nullif(promise_date, ''),        '(\d{2})$', '20\1'), 'DD-Mon-YYYY')   as promise_date,
        to_date(regexp_replace(nullif(scheduled_ship_date, ''), '(\d{2})$', '20\1'), 'DD-Mon-YYYY')   as scheduled_ship_date,
        to_date(regexp_replace(nullif(actual_ship_date, ''),    '(\d{2})$', '20\1'), 'DD-Mon-YYYY')   as actual_ship_date,
        to_date(regexp_replace(nullif(transaction_date, ''),    '(\d{2})$', '20\1'), 'DD-Mon-YYYY')   as transaction_date,

        /* =======================
           STATUS
           ======================= */
        order_status::text                                              as order_status,
        pick_status::text                                               as pick_status,
        credit_hold::text                                               as credit_hold,

        /* =======================
           PERFORMANCE
           ======================= */
        nullif(days_early_late, '')::numeric(10,2)                     as days_early_late,
        nullif(on_time_pct, '')::numeric(10,4)                         as on_time_pct,

        /* =======================
           FINANCIALS
           ======================= */
        nullif(cogs, '')::numeric(18,4)                                as cogs,
        nullif(sales_usd, '')::numeric(18,4)                           as sales_usd,
        nullif(gross_margin, '')::numeric(18,4)                        as gross_margin,
        nullif(margin_pct, '')::numeric(10,4)                          as margin_pct,

        /* =======================
           OTHER
           ======================= */
        promo_name::text                                                as promo_name,
        sales_representative::text                                      as sales_representative

    from base

)

select * from staged
