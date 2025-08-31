{{ config(materialized='view') }}

with base as (
  select
    order_id,
    case
      when order_delivered_customer_date is null then null
      when order_estimated_delivery_date is null then null
      when timestamp(order_delivered_customer_date) 
           <= timestamp(order_estimated_delivery_date) then 0
      else 1
    end as is_delayed
  from {{ ref('fact_orders') }}
)
select
  countif(is_delayed = 0) as on_time_orders,
  countif(is_delayed = 1) as delayed_orders,
  round(100 * countif(is_delayed = 0) / nullif(count(*),0), 2) as pct_on_time
from base
