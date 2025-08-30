MERGE `ready-de26.project_landing`.`order_items_landing_abdelrahman` T
USING `ready-de26.project_landing`.`order_items_stage_abdelrahman` S
ON  T.`order_id` = S.`order_id`
AND T.`order_item_id` = S.`order_item_id`
WHEN MATCHED THEN UPDATE SET
  T.`product_id`          = S.`product_id`,
  T.`seller_id`           = S.`seller_id`,
  T.`shipping_limit_date` = S.`shipping_limit_date`,
  T.`price`               = S.`price`,
  T.`freight_value`       = S.`freight_value`
WHEN NOT MATCHED THEN INSERT (
  `order_id`,
  `order_item_id`,
  `product_id`,
  `seller_id`,
  `shipping_limit_date`,
  `price`,
  `freight_value`
) VALUES (
  S.`order_id`,
  S.`order_item_id`,
  S.`product_id`,
  S.`seller_id`,
  S.`shipping_limit_date`,
  S.`price`,
  S.`freight_value`
);
order_id + order_item_id is