{{ config(materialized='table') }}

with src as (

    select * from {{ ref('stg_castle__po_open') }}

),

po_open as (

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
        buyer,

        /* =======================
           ITEM
           ======================= */
        product_primary_item_nbr,
        item_nbr,
        item_desc,
        product_form,
        product_commodity,
        product_item_type,
        supplier_item,

        /* =======================
           DATES
           ======================= */
        po_due_date,
        po_orig_due_date,
        action,
        action_date,

        /* =======================
           QUANTITIES & STATUS
           ======================= */
        po_status_code,
        po_uom,
        po_open_qty,
        po_open_lbs,
        po_ordered_qty,
        po_ordered_lbs,
        po_ordered_units,
        po_received_lbs,
        po_received_units,
        po_usd,
        open_usd,
        acceptance_type

    from src

)

select * from po_open
