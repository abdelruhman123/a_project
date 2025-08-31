{{ config(materialized='view') }}

-- Top customers by total order value
select
  dc.customer_unique_id,
  sum(foi.line_total) as total_order_value,
  count(distinct fo.order_id) as total_orders
from {{ ref('fact_order_items') }} foi
join {{ ref('fact_orders') }} fo using (order_id)
join {{ ref('dim_customers') }} dc
  on fo.customer_id = dc.customer_id
group by 1
order by total_order_value desc
limit 10;
