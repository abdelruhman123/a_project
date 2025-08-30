MERGE `ready-de26.project_landing`.`leads_qualified_landing_abdelrahman` T
USING `ready-de26.project_landing`.`leads_qualified_stage_abdelrahman` S
ON T.`mql_id` = S.`mql_id`
WHEN MATCHED THEN UPDATE SET
  T.`first_contact_date` = S.`first_contact_date`,
  T.`landing_page_id`    = S.`landing_page_id`,
  T.`origin`             = S.`origin`
WHEN NOT MATCHED THEN INSERT (
  `mql_id`,
  `first_contact_date`,
  `landing_page_id`,
  `origin`
) VALUES (
  S.`mql_id`,
  S.`first_contact_date`,
  S.`landing_page_id`,
  S.`origin`
);
