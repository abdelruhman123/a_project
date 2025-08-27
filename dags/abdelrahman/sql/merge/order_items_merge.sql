-- order_items_merge.sql
MERGE INTO `project_landing.order_items_stage_abdelrahman` AS target
USING `project_landing.order_items_staging_abdelrahman` AS source
ON target.order_item_id = source.order_item_id
WHEN MATCHED THEN
  UPDATE SET target.quantity = source.quantity, target.price = source.price
WHEN NOT MATCHED THEN
  INSERT (order_item_id, quantity, price, created_at)
  VALUES (source.order_item_id, source.quantity, source.price, source.created_at);