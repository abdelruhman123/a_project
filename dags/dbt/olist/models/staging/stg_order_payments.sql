{{ config(materialized='view') }}

-- Order payments staging view
select
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value
from {{ source('landing', 'order_payments_landing_abdelrahman') }};
