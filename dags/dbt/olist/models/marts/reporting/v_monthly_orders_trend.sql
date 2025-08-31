{{ config(materialized='view') }}

select
  format_date('%Y-%m', date(order_purchase_timestamp)) as month,
  count(distinct order_id) as total_orders
from {{ ref('fact_orders') }}
group by 1
order by 1
