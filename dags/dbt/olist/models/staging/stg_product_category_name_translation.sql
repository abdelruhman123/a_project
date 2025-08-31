{{ config(materialized='view') }}

-- Category translation staging view
select
  product_category_name,
  product_category_name_english
from {{ source('landing', 'product_category_name_translation_landing_abdelrahman') }}
