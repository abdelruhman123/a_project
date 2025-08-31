{{ config(materialized='view') }}

select
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value,
  updated_at_timestamp
from {{ source('project_landing','order_payments_landing_abdelrahman') }}
