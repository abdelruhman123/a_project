{{ config(materialized='view') }}

-- Order reviews staging view
select
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  review_creation_date,
  review_answer_timestamp
from {{ source('landing', 'order_reviews_landing_abdelrahman') }};
