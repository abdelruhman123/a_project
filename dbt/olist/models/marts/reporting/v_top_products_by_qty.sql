{{ config(materialized='view') }}

-- Top-selling products by quantity (count of order items)
select
  dp.product_id,
  dp.product_category_name,
  count(*) as total_items
from {{ ref('fact_order_items') }} foi
join {{ ref('dim_products') }} dp using (product_id)
group by 1,2
order by total_items desc
limit 100;
