-- product_category_name_translation_merge.sql
MERGE INTO `project_landing.product_category_name_translation_stage_abdelrahman` AS target
USING `project_landing.product_category_name_translation_staging_abdelrahman` AS source
ON target.product_category_id = source.product_category_id
WHEN MATCHED THEN
    UPDATE SET target.category_name = source.category_name
WHEN NOT MATCHED THEN
    INSERT (product_category_id, category_name)
    VALUES (source.product_category_id, source.category_name);
