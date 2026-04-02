{{ config(materialized='view') }}

-- 1) Pull raw HR worked hours exactly as loaded from Python ETL
--    Columns are already snake_case from pandas to_sql()
with raw as (

    select
        country,
        org,
        year,
        month,
        employee_id,
        employee_name,
        dept_code,
        regular_hrs,
        overtime_hrs,
        total_hrs

    from {{ source('hr', 'hr_workedhours') }}

),

-- 2) Cast types
cleaned as (

    select
        trim(country)                              as country,
        trim(org)                                  as org,
        cast(year  as integer)                     as year,
        cast(month as integer)                     as month,
        trim(cast(employee_id   as text))          as employee_id,
        trim(employee_name)                        as employee_name,
        trim(dept_code)                            as dept_code,
        cast(regular_hrs  as numeric(18,4))        as regular_hrs,
        cast(overtime_hrs as numeric(18,4))        as overtime_hrs,
        cast(total_hrs    as numeric(18,4))        as total_hrs

    from raw

)

select * from cleaned
