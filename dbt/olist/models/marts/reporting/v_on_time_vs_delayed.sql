{{ config(materialized='view') }}

-- On-time if delivered_date <= estimated_delivery_date
select
  sum(case when fo.order_delivered_customer_date <= fo.order_estimated_delivery_date then 1 else 0 end) as on_time_orders,
  sum(case when fo.order_delivered_customer_date >  fo.order_estimated_delivery_date then 1 else 0 end) as delayed_orders,
  safe_divide(
    sum(case when fo.order_delivered_customer_date <= fo.order_estimated_delivery_date then 1 else 0 end),
    count(*)
  ) as pct_on_time
from {{ ref('fact_orders') }} fo;
