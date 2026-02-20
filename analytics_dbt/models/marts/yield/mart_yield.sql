{{ config(materialized='table') }}

select *
from {{ ref('int_castle_yield_final') }}

union all

select *
from {{ ref('int_banner_yield_final') }}