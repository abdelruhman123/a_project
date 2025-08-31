{{ config(materialized='view') }}

select
  dp.product_category_name,
  avg(orv.review_score) as avg_review_score
from {{ ref('dim_products') }} dp
join {{ ref('fact_order_items') }} foi using (product_id)
join {{ ref('fact_orders') }} fo using (order_id)
join {{ source('landing','order_reviews_landing_abdelrahman') }} orv
  on orv.order_id = fo.order_id
group by 1
order by avg_review_score desc;
