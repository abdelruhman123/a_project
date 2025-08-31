{{ config(materialized='table') }}
select
  o.order_id,
  o.customer_id,
  o.order_status,
  o.order_purchase_timestamp,
  date(o.order_purchase_timestamp) as order_date,
  timestamp_diff(o.order_delivered_customer_date, o.order_purchase_timestamp, day) as delivery_days
from {{ ref('stg_orders') }} o;
