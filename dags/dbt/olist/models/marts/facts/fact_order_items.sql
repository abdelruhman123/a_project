{{ config(materialized='table') }}

select
  order_id,
  order_item_id,
  product_id,
  seller_id,
  price,
  freight_value,
  line_total
from {{ ref('int_order_items_priced') }}
