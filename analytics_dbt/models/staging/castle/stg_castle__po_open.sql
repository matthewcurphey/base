{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_castle__po_open') }}
),

staged as (

    select
        vendor_name::text                               as vendor_name,
        vendor_site_code::text                          as vendor_site_code,
        vendor_nbr::text                                as vendor_nbr,
        inv_org_code::text                              as inv_org_code,
        po_nbr::text                                    as po_nbr,
        po_line_nbr::text                               as po_line_nbr,
        supplier_item::text                             as supplier_item,
        item_nbr::text                                  as item_nbr,
        item_desc::text                                 as item_desc,
        product_grade::text                             as product_grade,
        product_shape::text                             as product_shape,
        po_open_lbs::numeric(18,4)                      as po_open_lbs,
        buyer::text                                     as buyer,
        product_item_type::text                         as product_item_type,
        product_core_type::text                         as product_core_type,
        product_lead_time::numeric(18,2)                as product_lead_time,
        open_usd::numeric(18,4)                         as open_usd,
        po_due_date::date                               as po_due_date,
        po_status_code::text                            as po_status_code,
        po_orig_due_date::date                          as po_orig_due_date,
        po_usd::numeric(18,4)                           as po_usd,
        po_ordered_lbs::numeric(18,4)                   as po_ordered_lbs,
        po_ordered_units::numeric(18,4)                 as po_ordered_units,
        po_open_qty::numeric(18,4)                      as po_open_qty,
        po_uom::text                                    as po_uom,
        description::text                               as description,
        product_form::text                              as product_form,
        product_primary_item_nbr::text                  as product_primary_item_nbr,
        product_commodity::text                         as product_commodity,
        po_ordered_qty::numeric(18,4)                   as po_ordered_qty,
        po_received_lbs::numeric(18,4)                  as po_received_lbs,
        acceptance_type::text                           as acceptance_type,
        action::text                                    as action,
        action_date::date                               as action_date,
        po_received_units::numeric(18,4)                as po_received_units,
        po_date::date                                   as po_date

    from base

)

select * from staged
