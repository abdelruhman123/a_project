{{ config(materialized='view') }}

select
  product_category_name,
  product_category_name_english
from {{ source('project_landing','product_category_name_translation_landing_abdelrahman') }}
