{{ config(materialized='view') }}

select
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  review_creation_date,
  review_answer_timestamp,
  updated_at_timestamp
from {{ source('project_landing','order_reviews_landing_abdelrahman') }}
