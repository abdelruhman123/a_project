{{ config(materialized='view') }}

select
  payment_type,
  sum(payment_value) as total_value
from {{ ref('fact_payments') }}
group by 1
order by total_value desc
