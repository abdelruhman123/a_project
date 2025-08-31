{{ config(materialized='view') }}

select
  format_date('%Y-%m', fo.order_date) as year_month,
  count(*) as total_orders
from {{ ref('fact_orders') }} fo
group by 1
order by year_month;
