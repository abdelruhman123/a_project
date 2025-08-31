{{ config(materialized='view') }}

select
  ds.seller_id,
  ds.seller_city,
  ds.seller_state,
  count(*) as order_items_count
from {{ ref('fact_order_items') }} foi
join {{ ref('dim_sellers') }} ds using (seller_id)
group by 1,2,3
order by order_items_count desc
limit 50;
