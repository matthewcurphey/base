{{ config(materialized='table') }}

select

    so_nbr,
    so_line as line,
    so_shipment as shp,
    inv_org_code as org,
    branch_name as branch,
    so_status as status,
    dj_1,
    dj_1_org as org_1,
    dj_1_status as status_1,
    dj_2,
    dj_2_org as org_2,
    dj_2_status as status_2,
    dj_3,
    dj_3_org as org_3,
    dj_3_status as status_3,
    reason_flagged

from {{ ref('int_castle__mcmaster_exception_dj_review') }}
