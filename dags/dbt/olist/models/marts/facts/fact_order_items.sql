{{ config(materialized='table') }}
select
  e.order_id,
  e.customer_id,
  e.order_status,
  e.order_purchase_timestamp,
  e.order_date,
  e.delivery_days
from {{ ref('int_orders_enriched') }} e;
