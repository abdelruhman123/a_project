MERGE `ready-de26.project_landing`.`orders_landing_abdelrahman` T
USING `ready-de26.project_landing`.`orders_stage_abdelrahman` S
ON T.`order_id` = S.`order_id`
WHEN MATCHED THEN UPDATE SET
  T.`customer_id`                   = S.`customer_id`,
  T.`order_status`                  = S.`order_status`,
  T.`order_purchase_timestamp`      = S.`order_purchase_timestamp`,
  T.`order_approved_at`             = S.`order_approved_at`,
  T.`order_delivered_carrier_date`  = S.`order_delivered_carrier_date`,
  T.`order_delivered_customer_date` = S.`order_delivered_customer_date`,
  T.`order_estimated_delivery_date` = S.`order_estimated_delivery_date`
WHEN NOT MATCHED THEN INSERT (
  `order_id`,
  `customer_id`,
  `order_status`,
  `order_purchase_timestamp`,
  `order_approved_at`,
  `order_delivered_carrier_date`,
  `order_delivered_customer_date`,
  `order_estimated_delivery_date`
) VALUES (
  S.`order_id`,
  S.`customer_id`,
  S.`order_status`,
  S.`order_purchase_timestamp`,
  S.`order_approved_at`,
  S.`order_delivered_carrier_date`,
  S.`order_delivered_customer_date`,
  S.`order_estimated_delivery_date`
);
