{{ config(materialized='table') }}

with transfers as (
    select *
    from {{ ref('stg_castle__transfers') }}
),

sales as (
    select *
    from {{ ref('int_foundation_castle__sales_salesorder') }}
),

transfer_rows as (
    select
        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        'castle'                                as company,
        t.order_number                          as so_nbr,
        t.sales_line_nbr                        as so_line,
        t.shipment_nbr                          as shipment_nbr,
        t.item_nbr                              as item_nbr,

        /* =======================
           LANE
           ======================= */
        t.shipping_inv_org_name                 as ship_from_branch,
        nullif(t.inv_org_name, '')              as ship_to_branch,
        t.shipping_inv_org_name || ' -> '
            || coalesce(nullif(t.inv_org_name, ''), 'Unknown')
                                                as lane,
        s.inv_org_code                          as ship_from_inv_org_code,

        /* =======================
           STATUS
           ======================= */
        t.sales_status                          as sales_status,
        t.sales_type                            as sales_type,
        s.line_transaction_type                 as line_transaction_type,
        (s.so_nbr is not null)                  as has_sales_match,

        /* =======================
           QUANTITIES & VALUE
           ======================= */
        t.lines                                 as lines,
        t.lines_shipped                         as lines_shipped,
        t.weight_lbs                            as weight_lbs,
        t.ordered_qty                           as ordered_qty,
        t.ordered_uom                           as ordered_uom,
        s.ordered_lbs                           as ordered_lbs,
        s.ordered_pcs                           as ordered_pcs,
        t.material_revenue                      as material_revenue,
        t.material_revenue_local                as material_revenue_local,
        s.material_cost_aac                     as material_cost_aac,

        /* =======================
           CUT SIZE
           ======================= */
        s.cut_uom                               as cut_uom,
        s.cut_shape                             as cut_shape,
        s.cut_width                             as cut_width,
        s.cut_length                            as cut_length,

        /* =======================
           KEY DATES
           (sales order_date = requisition_date and invoice_date = ship_date
            on every matched row, so they're not carried)
           ======================= */
        t.po_date                               as requisition_date,
        t.po_due_date                           as need_by_date,
        s.promise_date                          as promise_date,

        -- transfers' Last_Receipt_Date matches sales actual_ship_date on every
        -- matched row, so it's a ship date; sales fills it for any gaps
        coalesce(t.last_receipt_date, s.actual_ship_date)
                                                as ship_date,

        /* =======================
           DURATIONS (days)
           ======================= */
        t.po_due_date - t.po_date               as days_requisition_to_need_by,
        coalesce(t.last_receipt_date, s.actual_ship_date) - t.po_date
                                                as days_requisition_to_ship,

        /* =======================
           PRODUCT ATTRIBUTES
           ======================= */
        t.product_primary_item_nbr              as product_primary_item_nbr,
        s.product_item_description              as product_item_description,
        s.product_item_type                     as product_item_type,
        t.product_form                          as product_form,
        t.product_grade                         as product_grade,
        s.product_shape                         as product_shape,
        s.product_temper                        as product_temper,
        s.product_width                         as product_width,
        s.product_length                        as product_length,
        s.product_primary_dimension             as product_primary_dimension,
        s.product_stocking_uom                  as product_stocking_uom,
        s.product_commodity                     as product_commodity

    from transfers t
    left join sales s
        on  s.so_nbr       = t.order_number
        and s.so_line      = t.sales_line_nbr
        and s.shipment_nbr = t.shipment_nbr
)

select *
from transfer_rows
