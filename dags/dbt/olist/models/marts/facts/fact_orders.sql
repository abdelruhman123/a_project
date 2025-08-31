{{ config(materialized='table') }}

select
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  order_delivered_customer_date,
  order_estimated_delivery_date,
  delivery_days
from {{ ref('int_orders_enriched') }}
