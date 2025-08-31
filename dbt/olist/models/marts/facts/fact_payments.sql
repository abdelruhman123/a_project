{{ config(materialized='table') }}
select
  op.order_id,
  op.payment_sequential,
  op.payment_type,
  op.payment_installments,
  op.payment_value
from {{ ref('stg_order_payments') }} op;
