{{ config(materialized='view') }}

-- Geolocation staging view
select
  geolocation_zip_code_prefix,
  geolocation_lat,
  geolocation_lng,
  geolocation_city,
  geolocation_state
from {{ source('landing', 'geolocation_landing_abdelrahman') }}
