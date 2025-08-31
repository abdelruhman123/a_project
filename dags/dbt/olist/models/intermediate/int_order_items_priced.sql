{{ config(materialized='table') }}

select
  i.order_id,
  i.order_item_id,
  i.product_id,
  i.seller_id,
  i.price,
  i.freight_value,
  cast(coalesce(i.price,0) + coalesce(i.freight_value,0) as numeric) as line_total
from {{ ref('stg_order_items') }} i
