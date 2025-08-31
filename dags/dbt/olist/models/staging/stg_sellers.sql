{{ config(materialized='view') }}

-- Sellers staging view
select
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state
from {{ source('landing', 'sellers_landing_abdelrahman') }}
