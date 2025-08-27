-- products_merge.sql
MERGE INTO `project_landing.products_stage_abdelrahman` AS target
USING `project_landing.products_staging_abdelrahman` AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET target.product_name = source.product_name, 
               target.category_id = source.category_id, 
               target.product_price = source.product_price
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category_id, product_price)
    VALUES (source.product_id, source.product_name, source.category_id, source.product_price);
