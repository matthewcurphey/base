{{ config(materialized='view') }}

with raw as (

    select
        item_number,
        description,
        organization,
        sourcing_rule,
        source_type,
        supplier,
        supplier_site,
        source_organization,
        intransit_time,
        make_buy,
        purchasing_enabled,
        commodity,
        grade,
        shape,
        product_form,
        min_order_qty,
        order_incrementals,
        preprocessing,
        lead_time,
        postprocessing,
        fixed_days_supply,
        fixed_order_quantity,
        ss_method,
        bucket_days,
        percent,
        buyer,
        planner,
        cu_stock_status,
        abc_identifier,
        core_set,
        xxx_intrastat,
        cu_set,
        container_spec,
        spec_sourcing_rule,
        item_status,
        unit_of_measure
    from {{ source('oracle', 'castle_oracle_di') }}

),

cleaned as (

    select
        trim(item_number)           as item_number,
        trim(description)           as description,
        trim(organization)          as organization,
        trim(sourcing_rule)         as sourcing_rule,
        trim(source_type)           as source_type,
        trim(supplier)              as supplier,
        trim(supplier_site)         as supplier_site,
        trim(source_organization)   as source_organization,
        trim(intransit_time)        as intransit_time,
        trim(make_buy)              as make_buy,
        trim(purchasing_enabled)    as purchasing_enabled,
        trim(commodity)             as commodity,
        trim(grade)                 as grade,
        trim(shape)                 as shape,
        trim(product_form)          as product_form,
        trim(min_order_qty)         as min_order_qty,
        trim(order_incrementals)    as order_incrementals,
        trim(preprocessing)         as preprocessing,
        trim(lead_time)             as lead_time,
        trim(postprocessing)        as postprocessing,
        trim(fixed_days_supply)     as fixed_days_supply,
        trim(fixed_order_quantity)  as fixed_order_quantity,
        trim(ss_method)             as ss_method,
        trim(bucket_days)           as bucket_days,
        trim(percent)               as percent,
        trim(buyer)                 as buyer,
        trim(planner)               as planner,
        trim(cu_stock_status)       as cu_stock_status,
        trim(abc_identifier)        as abc_identifier,
        trim(core_set)              as core_set,
        trim(xxx_intrastat)         as xxx_intrastat,
        trim(cu_set)                as cu_set,
        trim(container_spec)        as container_spec,
        trim(spec_sourcing_rule)    as spec_sourcing_rule,
        trim(item_status)           as item_status,
        trim(unit_of_measure)       as unit_of_measure
    from raw

)

select * from cleaned
