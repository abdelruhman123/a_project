{{ config(materialized='view') }}

select
  o.customer_id,
  sum(oi.line_total) as total_spend
from {{ ref('fact_orders') }} o
join {{ ref('fact_order_items') }} oi using(order_id)
group by 1
order by total_spend desc
