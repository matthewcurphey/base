{{ config(materialized='view') }}

with base as (
    select * from {{ ref('base_castle_oracle__di') }}
),

staged as (

    select

        item_number::text                           as item_number,
        description::text                           as description,
        organization::text                          as organization,
        sourcing_rule::text                         as sourcing_rule,
        source_type::text                           as source_type,
        supplier::text                              as supplier,
        supplier_site::text                         as supplier_site,
        source_organization::text                   as source_organization,
        intransit_time::numeric(10,2)               as intransit_time,
        make_buy::text                              as make_buy,
        purchasing_enabled::text                    as purchasing_enabled,
        commodity::text                             as commodity,
        grade::text                                 as grade,
        shape::text                                 as shape,
        product_form::text                          as product_form,
        min_order_qty::numeric(18,6)                as min_order_qty,
        order_incrementals::numeric(18,6)           as order_incrementals,
        preprocessing::numeric(10,2)                as preprocessing,
        lead_time::numeric(10,2)                    as lead_time,
        postprocessing::numeric(10,2)               as postprocessing,
        fixed_days_supply::numeric(10,2)            as fixed_days_supply,
        fixed_order_quantity::numeric(18,6)         as fixed_order_quantity,
        ss_method::text                             as ss_method,
        bucket_days::numeric(10,2)                  as bucket_days,
        percent::numeric(10,4)                      as percent,
        buyer::text                                 as buyer,
        planner::text                               as planner,
        cu_stock_status::text                       as cu_stock_status,
        abc_identifier::text                        as abc_identifier,
        core_set::text                              as core_set,
        xxx_intrastat::text                         as xxx_intrastat,
        cu_set::text                                as cu_set,
        container_spec::text                        as container_spec,
        spec_sourcing_rule::text                    as spec_sourcing_rule,
        item_status::text                           as item_status,
        unit_of_measure::text                       as unit_of_measure

    from base

)

select * from staged
