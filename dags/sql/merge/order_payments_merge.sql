MERGE `ready-de26.project_landing`.`order_payments_landing_abdelrahman` T
USING `ready-de26.project_landing`.`order_payments_stage_abdelrahman` S
ON  T.`order_id` = S.`order_id`
AND T.`payment_sequential` = S.`payment_sequential`
WHEN MATCHED THEN UPDATE SET
  T.`payment_type`        = S.`payment_type`,
  T.`payment_installments`= S.`payment_installments`,
  T.`payment_value`       = S.`payment_value`
WHEN NOT MATCHED THEN INSERT (
  `order_id`,
  `payment_sequential`,
  `payment_type`,
  `payment_installments`,
  `payment_value`
) VALUES (
  S.`order_id`,
  S.`payment_sequential`,
  S.`payment_type`,
  S.`payment_installments`,
  S.`payment_value`
);
order_id + payment_sequential is the primary key