{{ config(materialized='view') }}

select * from {{ ref('int_banner__salesinvoice') }}

union all

select * from {{ ref('int_castle__salesinvoice') }}
