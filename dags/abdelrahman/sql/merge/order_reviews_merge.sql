-- order_reviews_merge.sql
MERGE INTO `project_landing.order_reviews_stage_abdelrahman` AS target
USING `project_landing.order_reviews_staging_abdelrahman` AS source
ON target.review_id = source.review_id
WHEN MATCHED THEN
    UPDATE SET target.rating = source.rating, target.review_text = source.review_text
WHEN NOT MATCHED THEN
    INSERT (review_id, order_id, rating, review_text)
    VALUES (source.review_id, source.order_id, source.rating, source.review_text);
-- Note: Assuming order_id is part of the staging table for foreign key reference