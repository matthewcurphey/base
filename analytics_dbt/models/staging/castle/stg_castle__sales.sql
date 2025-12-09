{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_castle__sales') }}
),

staged as (

    select
        branch_name::text                       as branch_name,
        inv_org_code::text                      as inv_org_code,
        sales_status::text                      as sales_status,
        sales_type::text                        as sales_type,
        sales_order_nbr::text                   as sales_order_nbr,
        sales_line_nbr::text                    as sales_line_nbr,
        shipment_nbr::text                      as shipment_nbr,
        line_transaction_type::text             as line_transaction_type,

        order_date::date                        as order_date,
        invoice_date::date                      as invoice_date,
        actual_ship_date::date                  as actual_ship_date,
        quote_date::date                        as quote_date,
        request_date::date                      as request_date,
        promise_date::date                      as promise_date,

        ordered_qty::numeric(18,6)              as ordered_qty,
        ordered_uom::text                       as ordered_uom,
        ordered_pcs::numeric(18,6)              as ordered_pcs,

        freight_cost::numeric(18,4)             as freight_cost,
        freight_revenue_usd::numeric(18,4)      as freight_revenue_usd,
        material_revenue::numeric(18,4)         as material_revenue,
        proc_rev_usd::numeric(18,4)             as proc_rev_usd,
        material_cost_aac::numeric(18,4)        as material_cost_aac,
        material_overhead_cost::numeric(18,4)   as material_overhead_cost,
        outside_processing_cost::numeric(18,4)  as outside_processing_cost,
        resource_cost_usd::numeric(18,4)        as resource_cost_usd,
        total_gross_profit_usd::numeric(18,4)   as total_gross_profit_usd,
        list_price_per_lbs_gross::numeric(18,6) as list_price_per_lbs_gross,
        price_per_lbs_gross::numeric(18,6)      as price_per_lbs_gross,
        total_sales_usd::numeric(18,4)          as total_sales_usd,
        weight_lbs::numeric(18,4)               as weight_lbs,
        gross_weight_lbs::numeric(18,4)         as gross_weight_lbs,
        invoiced_lbs::numeric(18,4)             as invoiced_lbs,
        invoiced_pcs::numeric(18,4)             as invoiced_pcs,
        invoiced_qty::numeric(18,4)             as invoiced_qty,
        uom::text                               as uom,
        ordered_lbs::numeric(18,4)              as ordered_lbs,
        matl_gp_usd::numeric(18,4)              as matl_gp_usd,
        tgp_pct::numeric(18,6)                   as tgp_pct,
        mgp_pct::numeric(18,6)                   as mgp_pct,
        absorption_cost_usd::numeric(18,4)      as absorption_cost_usd,

        product_item_nbr::text                  as product_item_nbr,
        product_item_type::text                 as product_item_type,
        product_primary_item_nbr::text          as product_primary_item_nbr,
        product_form::text                      as product_form,
        product_grade::text                     as product_grade,
        product_customer::text                  as product_customer,
        product_shape::text                     as product_shape,
        product_stocking_uom::text              as product_stocking_uom,
        product_primary_dimension::numeric(18,6)         as product_primary_dimension,
        product_temper::text                    as product_temper,

        product_length::numeric(18,6)           as product_length,
        product_width::numeric(18,6)            as product_width,

        product_commodity::text                 as product_commodity,
        product_item_description::text          as product_item_description,
        product_source_type::text               as product_source_type,

        ship_to_customer_name::text             as ship_to_customer_name,
        ship_to_customer_nbr::text              as ship_to_customer_nbr,
        sold_to_customer_name::text             as sold_to_customer_name,
        sold_to_customer_nbr::text              as sold_to_customer_nbr

    from base
)

select * from staged
