{{ config(materialized='table') }}
with RECURSIVE d as (
  select date('2016-01-01') as dt
  union all
  select date_add(dt, interval 1 day) from d where dt < date('2018-12-31')
)
select
  dt as date_day,
  extract(year from dt) as year,
  extract(month from dt) as month,
  format_date('%Y-%m', dt) as year_month
from d;
