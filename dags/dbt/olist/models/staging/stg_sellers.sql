{{ config(materialized='view') }}

select
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state,
  updated_at_timestamp
from {{ source('project_landing','sellers_landing_abdelrahman') }}
