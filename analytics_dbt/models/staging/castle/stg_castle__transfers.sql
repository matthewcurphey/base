{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_castle__transfers') }}
),

staged as (

    select
        inv_org_name::text                              as inv_org_name,
        product_primary_item_nbr::text                  as product_primary_item_nbr,
        item_nbr::text                                  as item_nbr,
        product_form::text                              as product_form,
        product_grade::text                             as product_grade,
        lines::numeric(18,4)                            as lines,
        weight_lbs::numeric(18,4)                       as weight_lbs,
        ordered_qty::numeric(18,4)                      as ordered_qty,
        material_revenue::numeric(18,4)                 as material_revenue,
        material_revenue_local::numeric(18,4)           as material_revenue_local,
        lines_shipped::numeric(18,4)                    as lines_shipped,
        ordered_uom::text                               as ordered_uom,
        sales_status::text                              as sales_status,
        sales_type::text                                as sales_type,
        shipment_nbr::text                              as shipment_nbr,
        sales_line_nbr::text                            as sales_line_nbr,
        order_number::text                              as order_number,
        shipping_inv_org_name::text                     as shipping_inv_org_name,
        po_date::date                                   as po_date,
        po_due_date::date                               as po_due_date,
        last_receipt_date::date                         as last_receipt_date

    from base

)

select * from staged
