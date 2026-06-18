{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_castle__po_receipts') }}
),

staged as (

    select
        po_receipt_date::date                           as po_receipt_date,
        vendor_name::text                               as vendor_name,
        vendor_site_code::text                          as vendor_site_code,
        vendor_nbr::text                                as vendor_nbr,
        inv_org_code::text                              as inv_org_code,
        po_nbr::text                                    as po_nbr,
        po_line_nbr::text                               as po_line_nbr,
        transaction_type::text                          as transaction_type,
        item_nbr::text                                  as item_nbr,
        product_item_type::text                         as product_item_type,
        item_desc::text                                 as item_desc,
        product_commodity::text                         as product_commodity,
        product_form::text                              as product_form,
        product_shape::text                             as product_shape,
        product_grade::text                             as product_grade,
        product_core_type::text                         as product_core_type,
        poh_po_deliver_lbs::numeric(18,4)               as poh_po_deliver_lbs,
        poh_po_deliver_usd::numeric(18,4)               as poh_po_deliver_usd,
        poh_ordered_qty_lbs::numeric(18,4)              as poh_ordered_qty_lbs,
        po_ordered_value_usd::numeric(18,4)             as po_ordered_value_usd,
        supplier_item::text                             as supplier_item,
        po_received_qty::numeric(18,4)                  as po_received_qty,
        po_uom::text                                    as po_uom,
        poh_po_received_lbs::numeric(18,4)              as poh_po_received_lbs,
        poh_po_receive_usd::numeric(18,4)               as poh_po_receive_usd,
        product_planner_name::text                      as product_planner_name,
        po_date::date                                   as po_date

    from base

)

select * from staged
