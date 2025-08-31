{{ config(materialized='table') }}
select
  i.order_id,
  i.order_item_id,
  i.product_id,
  i.seller_id,
  i.price,
  i.freight_value,
  i.line_total
from {{ ref('int_order_items_priced') }} i
