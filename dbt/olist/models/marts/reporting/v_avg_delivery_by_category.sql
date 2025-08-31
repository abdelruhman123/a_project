{{ config(materialized='view') }}

-- Average delivery time (days) per product category
select
  dp.product_category_name,
  avg(fo.delivery_days) as avg_delivery_days
from {{ ref('fact_orders') }} fo
join {{ ref('fact_order_items') }} foi using (order_id)
join {{ ref('dim_products') }} dp using (product_id)
group by 1
order by avg_delivery_days;
