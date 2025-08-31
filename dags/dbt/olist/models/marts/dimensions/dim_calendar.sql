{{ config(materialized='table') }}

with days as (
  select day
  from unnest(
    generate_date_array(date('2016-01-01'), date_add(current_date(), interval 365 day))
  ) as day
)

select
  day                                    as date_day,
  extract(year  from day)                as year,
  extract(quarter from day)              as quarter,
  extract(month  from day)               as month,
  format_date('%Y-%m', day)              as year_month,
  extract(week   from day)               as week,
  extract(day    from day)               as day_of_month,
  format_date('%A', day)                 as day_name
from days
