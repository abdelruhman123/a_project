{{ config(materialized='table') }}
select
  c.customer_id,
  c.customer_unique_id,
  c.customer_zip_code_prefix,
  g.geolocation_city as customer_city,
  g.geolocation_state as customer_state
from {{ source('landing','customers_landing_abdelrahman') }} c
left join {{ source('landing','geolocation_landing_abdelrahman') }} g
  on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix;
