{{ config(materialized='table') }}

with c as (
  select * from {{ ref('stg_customers') }}
),
g as (
  select * from {{ ref('stg_geolocation') }}
)

select
  c.customer_id,
  c.customer_unique_id,
  c.customer_city,
  c.customer_state,
  c.customer_zip_code_prefix,
  
  g.geolocation_lat  as lat,
  g.geolocation_lng  as lng
from c
left join g
  on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
