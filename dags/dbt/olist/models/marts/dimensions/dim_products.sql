{{ config(materialized='table') }}

select
  p.product_id,
  coalesce(p.product_category_name, 'unknown') as product_category_name,

  cast(p.product_name_lenght as int64)                as product_name_length,
  cast(p.product_description_lenght as int64)         as product_description_length,
  cast(p.product_photos_qty as int64)                 as product_photos_qty,
  cast(p.product_weight_g as int64)                   as product_weight_g,
  cast(p.product_length_cm as int64)                  as product_length_cm,
  cast(p.product_height_cm as int64)                  as product_height_cm,
  cast(p.product_width_cm as int64)                   as product_width_cm
from {{ ref('stg_products') }} p
