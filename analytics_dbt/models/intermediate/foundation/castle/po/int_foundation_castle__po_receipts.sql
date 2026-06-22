{{ config(materialized='table') }}

with src as (

    select * from {{ ref('stg_castle__po_receipts') }}

),

po_receipts as (

    select

        /* =======================
           IDENTIFIERS
           ======================= */
        inv_org_code,
        po_nbr,
        po_line_nbr,
        po_date,

        /* =======================
           VENDOR
           ======================= */
        vendor_name,
        vendor_nbr,
        vendor_site_code,
        product_planner_name,

        /* =======================
           ITEM
           ======================= */
        item_nbr,
        item_desc,
        product_form,
        product_commodity,
        product_item_type,
        product_core_type,
        product_shape,
        product_grade,
        supplier_item,

        /* =======================
           RECEIPT DETAIL
           ======================= */
        po_receipt_date,
        transaction_type,
        po_uom,
        po_received_qty,
        poh_po_received_lbs,
        poh_po_receive_usd,
        poh_po_deliver_lbs,
        poh_po_deliver_usd,
        poh_ordered_qty_lbs,
        po_ordered_value_usd

    from src

)

select * from po_receipts
