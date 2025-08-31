{{ config(materialized='view') }}

with per_cust as (
  select
    customer_id,
    count(distinct order_id) as orders_per_customer
  from {{ ref('fact_orders') }}
  group by 1
)
select
  avg(orders_per_customer) as avg_orders_per_customer
from per_cust
