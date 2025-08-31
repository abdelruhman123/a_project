{{ config(materialized='view') }}

select
  format_date('%Y-%m', fo.order_date) as year_month,
  sum(foi.line_total) as revenue
from {{ ref('fact_orders') }} fo
join {{ ref('fact_order_items') }} foi using (order_id)
group by 1
order by year_month;
