{{ config(materialized='table') }}
select
  p.product_id,
  coalesce(t.product_category_name_english, p.product_category_name) as product_category_name,
  p.product_name_length,
  p.product_description_length,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
from {{ source('landing','products_landing_abdelrahman') }} p
left join {{ source('landing','product_category_name_translation_landing_abdelrahman') }} t
  on p.product_category_name = t.product_category_name
