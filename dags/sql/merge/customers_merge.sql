MERGE `ready-de26.project_landing`.`customers_landing_abdelrahman` T
USING `ready-de26.project_landing`.`customers_stage_abdelrahman` S
ON T.`customer_id` = S.`customer_id`
WHEN MATCHED THEN UPDATE SET
  T.`customer_unique_id`       = S.`customer_unique_id`,
  T.`customer_zip_code_prefix` = S.`customer_zip_code_prefix`,
  T.`customer_city`            = S.`customer_city`,
  T.`customer_state`           = S.`customer_state`
WHEN NOT MATCHED THEN INSERT (
  `customer_id`,
  `customer_unique_id`,
  `customer_zip_code_prefix`,
  `customer_city`,
  `customer_state`
) VALUES (
  S.`customer_id`,
  S.`customer_unique_id`,
  S.`customer_zip_code_prefix`,
  S.`customer_city`,
  S.`customer_state`
);
