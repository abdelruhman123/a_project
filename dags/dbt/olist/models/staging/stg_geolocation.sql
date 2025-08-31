{{ config(materialized='view') }}

select
  geolocation_zip_code_prefix,
  geolocation_lat,
  geolocation_lng,
  geolocation_city,
  geolocation_state
from {{ source('project_landing','geolocation_landing_abdelrahman') }}
