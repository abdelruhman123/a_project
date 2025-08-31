{{ config(materialized='table') }}
select
  oi.order_id,
  oi.order_item_id,
  oi.product_id,
  oi.seller_id,
  oi.price,
  oi.freight_value,
  (oi.price + oi.freight_value) as line_total
from {{ ref('stg_order_items') }} oi;
