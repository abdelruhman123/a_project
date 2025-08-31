{{ config(materialized='view') }}

select
  fp.payment_type,
  sum(fp.payment_value) as revenue
from {{ ref('fact_payments') }} fp
group by 1
order by revenue desc;
