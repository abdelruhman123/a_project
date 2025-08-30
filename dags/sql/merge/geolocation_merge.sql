MERGE `ready-de26.project_landing`.`geolocation_landing_abdelrahman` T
USING `ready-de26.project_landing`.`geolocation_stage_abdelrahman` S
ON T.`geolocation_zip_code_prefix` = S.`geolocation_zip_code_prefix`
WHEN MATCHED THEN UPDATE SET
  T.`geolocation_lat`   = S.`geolocation_lat`,
  T.`geolocation_lng`   = S.`geolocation_lng`,
  T.`geolocation_city`  = S.`geolocation_city`,
  T.`geolocation_state` = S.`geolocation_state`
WHEN NOT MATCHED THEN INSERT (
  `geolocation_zip_code_prefix`,
  `geolocation_lat`,
  `geolocation_lng`,
  `geolocation_city`,
  `geolocation_state`
) VALUES (
  S.`geolocation_zip_code_prefix`,
  S.`geolocation_lat`,
  S.`geolocation_lng`,
  S.`geolocation_city`,
  S.`geolocation_state`
);
