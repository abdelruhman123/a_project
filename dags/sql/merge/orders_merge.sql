-- orders_merge.sql
MERGE INTO `project_landing.orders_stage_abdelrahman` AS target
USING `project_landing.orders_staging_abdelrahman` AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET target.customer_id = source.customer_id, 
               target.total_amount = source.total_amount, 
               target.order_date = source.order_date
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, total_amount, order_date)
    VALUES (source.order_id, source.customer_id, source.total_amount, source.order_date);
