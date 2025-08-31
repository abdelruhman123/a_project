{{ config(materialized='view') }}

-- Average number of orders per customer
with per_customer as (
  select customer_id, count(*) as order_count
  from {{ ref('fact_orders') }}
  group by customer_id
)
select avg(order_count) as avg_orders_per_customer
from per_customer;
