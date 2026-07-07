{{ config(materialized='table') }}

with base as (

    select * from {{ ref('int_foundation_castle__sales_production') }}
    where (ship_to_customer_nbr = '4872' or sold_to_customer_nbr = '4872')
      and dj_nbr is not null
      and job_status in ('Released', 'On Hold', 'Failed Close')

),

/* =====================================================
   ACTIVE DJ COUNT PER LINE
   Regardless of order status — a line can carry more
   than one DJ; the exception is when more than one of
   them is simultaneously active. Max observed today is 2,
   so 3 pivot slots below gives headroom.
===================================================== */

active_dj_counts as (

    select
        so_nbr,
        so_line,
        so_shipment,
        count(distinct dj_nbr)      as active_dj_count
    from base
    group by so_nbr, so_line, so_shipment

),

/* =====================================================
   FLAGGED
   Two scenarios, flagged independently (a line can hit
   both):

   1. is_terminal_order — the SO is no longer open (shipped,
      invoiced, or cancelled) but an active DJ is still
      attached. Terminal = anything except (Valid, Order);
      cancelled orders keep sales_type = 'Order', so
      sales_status must be checked too, not sales_type alone.

   2. is_multi_active_dj — more than one DJ attached to the
      line is simultaneously active. Having multiple DJs on
      a line is normal; more than one active at once isn't.
===================================================== */

flagged as (

    select
        base.*,
        c.active_dj_count,
        not (base.sales_status = 'Valid' and base.sales_type = 'Order')  as is_terminal_order,
        c.active_dj_count > 1                                            as is_multi_active_dj
    from base
    inner join active_dj_counts c
        on  base.so_nbr      = c.so_nbr
        and base.so_line     = c.so_line
        and base.so_shipment = c.so_shipment
    where not (base.sales_status = 'Valid' and base.sales_type = 'Order')
       or c.active_dj_count > 1

),

ranked as (

    select
        *,
        row_number() over (
            partition by so_nbr, so_line, so_shipment
            order by dj_nbr
        )                                                                as dj_rank
    from flagged

)

/* =====================================================
   ONE ROW PER SO/LINE/SHIPMENT
   Active DJs pivoted into fixed slots so someone can dig
   in without joining anything — dj_2/dj_3 are blank when
   there's only one active DJ on the line.
===================================================== */

select

    so_nbr,
    so_line,
    so_shipment,
    max(inv_org_code)                                                     as inv_org_code,
    max(branch_name)                                                      as branch_name,

    max(case
        when sales_status = 'Cancelled'                        then 'Cancelled'
        when sales_status = 'Valid' and sales_type = 'Invoice'  then 'Invoiced'
        when sales_status = 'Valid' and sales_type = 'Order'    then 'Open'
        else sales_status || ' / ' || sales_type
    end)                                                                 as so_status,

    max(case when dj_rank = 1 then dj_nbr     end)                      as dj_1,
    max(case when dj_rank = 1 then dj_org     end)                      as dj_1_org,
    max(case when dj_rank = 1 then job_status end)                      as dj_1_status,

    max(case when dj_rank = 2 then dj_nbr     end)                      as dj_2,
    max(case when dj_rank = 2 then dj_org     end)                      as dj_2_org,
    max(case when dj_rank = 2 then job_status end)                      as dj_2_status,

    max(case when dj_rank = 3 then dj_nbr     end)                      as dj_3,
    max(case when dj_rank = 3 then dj_org     end)                      as dj_3_org,
    max(case when dj_rank = 3 then job_status end)                      as dj_3_status,

    array_to_string(array_remove(array[
        case when bool_or(is_terminal_order)  then 'Terminal order with active DJ' end,
        case when bool_or(is_multi_active_dj) then 'Multiple active DJs'            end
    ], null), '; ')                                                      as reason_flagged

from ranked
group by so_nbr, so_line, so_shipment
order by so_nbr, so_line, so_shipment
