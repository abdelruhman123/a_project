MERGE `ready-de26.project_landing`.`sellers_landing_abdelrahman` T
USING `ready-de26.project_landing`.`sellers_stage_abdelrahman` S
ON T.`seller_id` = S.`seller_id`
WHEN MATCHED THEN UPDATE SET
  T.`seller_zip_code_prefix` = S.`seller_zip_code_prefix`,
  T.`seller_city`            = S.`seller_city`,
  T.`seller_state`           = S.`seller_state`
WHEN NOT MATCHED THEN INSERT (
  `seller_id`,
  `seller_zip_code_prefix`,
  `seller_city`,
  `seller_state`
) VALUES (
  S.`seller_id`,
  S.`seller_zip_code_prefix`,
  S.`seller_city`,
  S.`seller_state`
);
