{{ config(materialized='view') }}

-- Revenue by product category
select
  dp.product_category_name,
  sum(foi.line_total) as revenue
from {{ ref('fact_order_items') }} foi
join {{ ref('dim_products') }} dp using (product_id)
group by 1
order by revenue desc;
