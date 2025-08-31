{{ config(materialized='view') }}

select
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  updated_at_timestamp
from {{ source('landing','customers_landing_abdelrahman') }}
