import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "ready-de26"
BUCKET = "ready-labs-postgres-to-gcs"
BQ_STAGE_DATASET = "project_landing"
BQ_LANDING_DATASET = "project_landing"

TABLES = {
    "order_items": "public.order_items",
    "orders": "public.orders",
    "products": "public.products",
    "order_reviews": "public.order_reviews",
    "product_category_name_translation": "public.product_category_name_translation",
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SQL_FOLDER = os.path.join(os.path.dirname(__file__), "sql", "merge")

with DAG(
    dag_id="extract_load_postgres_to_gcs",  
    default_args=default_args,
    description="ETL: Postgres -> GCS -> Stage -> Landing (range window, create-if-missing)",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 23),   
    end_date=datetime(2025, 8, 27),    
    catchup=True,
    max_active_runs=1,
    tags=["abdelrahman", "db1_incremental_like_first"],
) as dag:

    for table_name, pg_table in TABLES.items():
        
        extract_to_gcs = PostgresToGCSOperator(
            task_id=f"extract_{table_name}_to_gcs",
            postgres_conn_id="postgres_db_abdelrahman",
            sql=f"""
                SELECT * FROM {pg_table}
                WHERE updated_at_timestamp >= '{{{{ macros.ds_add(ds, -1) }}}}'
                  AND updated_at_timestamp <  '{{{{ ds }}}}'
            """,
            bucket=BUCKET,
            filename=(
                f"abdelrahman_db1/{table_name}/"
                f"dt={{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/data.json"
            ),
            export_format="json",
            gzip=False,
        )

        load_to_stage = GCSToBigQueryOperator(
            task_id=f"load_{table_name}_to_stage",
            bucket=BUCKET,
            source_objects=[
                f"abdelrahman_db1/{table_name}/dt={{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/data.json"
            ],
            destination_project_dataset_table=(
                f"{PROJECT_ID}.{BQ_STAGE_DATASET}.{table_name}_stage_abdelrahman"
            ),
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            autodetect=True,
        )

        create_landing_if_missing = BigQueryInsertJobOperator(
            task_id=f"create_{table_name}_landing_if_missing",
            configuration={
                "query": {
                    "query": f"""
                        CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{BQ_LANDING_DATASET}`;

                        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BQ_LANDING_DATASET}.{table_name}_landing_abdelrahman`
                        AS SELECT * FROM `{PROJECT_ID}.{BQ_STAGE_DATASET}.{table_name}_stage_abdelrahman`
                        WHERE 1=0;
                    """,
                    "useLegacySql": False,
                }
            },
            location="US", 
        )

        #  4: Read MERGE SQL from file (
        sql_file_path = os.path.join(SQL_FOLDER, f"{table_name}_merge.sql")
        with open(sql_file_path, "r") as f:
            merge_sql = f.read()


        merge_to_landing = BigQueryInsertJobOperator(
            task_id=f"merge_{table_name}_to_landing",
            configuration={
                "query": {
                    "query": merge_sql,
                    "useLegacySql": False,
                }
            },
            location="US",
        )

        # ========= Dependencies =========
        extract_to_gcs >> load_to_stage >> create_landing_if_missing >> merge_to_landing
