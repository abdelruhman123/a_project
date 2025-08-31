{{ config(materialized='view') }}

select
  payment_type,
  count(*) as tx_count
from {{ ref('fact_payments') }}
group by 1
order by tx_count desc;
