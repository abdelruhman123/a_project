{{ config(materialized='view') }}

-- Customers staging view
select
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
from {{ source('landing', 'customers_landing_abdelrahman') }};
