{{ config(materialized='view') }}

select
  format_timestamp('%Y-%m', order_purchase_timestamp) as year_month,
  sum(oi.line_total) as revenue
from {{ ref('fact_orders') }} o
join {{ ref('fact_order_items') }} oi using(order_id)
group by 1
order by 1
