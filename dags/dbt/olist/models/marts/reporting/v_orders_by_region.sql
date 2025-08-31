{{ config(materialized='view') }}

select
  dc.customer_state,
  count(distinct fo.order_id) as total_orders
from {{ ref('fact_orders') }} fo
join {{ ref('dim_customers') }} dc
  on fo.customer_id = dc.customer_id
group by 1
order by total_orders desc;
