MERGE `ready-de26.project_landing`.`product_category_name_translation_landing_abdelrahman` T
USING `ready-de26.project_landing`.`product_category_name_translation_stage_abdelrahman` S
ON T.`product_category_name` = S.`product_category_name`
WHEN MATCHED THEN UPDATE SET
  T.`product_category_name_english` = S.`product_category_name_english`
WHEN NOT MATCHED THEN INSERT (
  `product_category_name`,
  `product_category_name_english`
) VALUES (
  S.`product_category_name`,
  S.`product_category_name_english`
);
