{{ config(materialized='view') }}

select
  order_id,
  order_item_id,
  product_id,
  seller_id,
  shipping_limit_date,
  price,
  freight_value,
  updated_at_timestamp
from {{ source('landing','order_items_landing_abdelrahman') }}
