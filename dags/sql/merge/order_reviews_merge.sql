MERGE `ready-de26.project_landing`.`order_reviews_landing_abdelrahman` T
USING `ready-de26.project_landing`.`order_reviews_stage_abdelrahman` S
ON T.`review_id` = S.`review_id`
WHEN MATCHED THEN UPDATE SET
  T.`order_id`               = S.`order_id`,
  T.`review_score`           = S.`review_score`,
  T.`review_comment_title`   = S.`review_comment_title`,
  T.`review_comment_message` = S.`review_comment_message`,
  T.`review_creation_date`   = S.`review_creation_date`,
  T.`review_answer_timestamp`= S.`review_answer_timestamp`
WHEN NOT MATCHED THEN INSERT (
  `review_id`,
  `order_id`,
  `review_score`,
  `review_comment_title`,
  `review_comment_message`,
  `review_creation_date`,
  `review_answer_timestamp`
) VALUES (
  S.`review_id`,
  S.`order_id`,
  S.`review_score`,
  S.`review_comment_title`,
  S.`review_comment_message`,
  S.`review_creation_date`,
  S.`review_answer_timestamp`
);
