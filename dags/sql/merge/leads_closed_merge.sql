MERGE `ready-de26.project_landing`.`leads_closed_landing_abdelrahman` T
USING `ready-de26.project_landing`.`leads_closed_stage_abdelrahman` S
ON T.`mql_id` = S.`mql_id`
WHEN MATCHED THEN UPDATE SET
  T.`seller_id`                   = S.`seller_id`,
  T.`sdr_id`                      = S.`sdr_id`,
  T.`sr_id`                       = S.`sr_id`,
  T.`won_date`                    = S.`won_date`,
  T.`business_segment`            = S.`business_segment`,
  T.`lead_type`                   = S.`lead_type`,
  T.`lead_behaviour_profile`      = S.`lead_behaviour_profile`,
  T.`has_company`                 = S.`has_company`,
  T.`has_gtin`                    = S.`has_gtin`,
  T.`average_stock`               = S.`average_stock`,
  T.`business_type`               = S.`business_type`,
  T.`declared_product_catalog_size` = S.`declared_product_catalog_size`,
  T.`declared_monthly_revenue`    = S.`declared_monthly_revenue`
WHEN NOT MATCHED THEN INSERT (
  `mql_id`,
  `seller_id`,
  `sdr_id`,
  `sr_id`,
  `won_date`,
  `business_segment`,
  `lead_type`,
  `lead_behaviour_profile`,
  `has_company`,
  `has_gtin`,
  `average_stock`,
  `business_type`,
  `declared_product_catalog_size`,
  `declared_monthly_revenue`
) VALUES (
  S.`mql_id`,
  S.`seller_id`,
  S.`sdr_id`,
  S.`sr_id`,
  S.`won_date`,
  S.`business_segment`,
  S.`lead_type`,
  S.`lead_behaviour_profile`,
  S.`has_company`,
  S.`has_gtin`,
  S.`average_stock`,
  S.`business_type`,
  S.`declared_product_catalog_size`,
  S.`declared_monthly_revenue`
);
