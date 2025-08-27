import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Info for GCP and BigQuery
PROJECT_ID = "ready-de26"
BUCKET = "ready-labs-postgres-to-gcs"
BQ_STAGE_DATASET = "project_landing"  
BQ_LANDING_DATASET = "project_landing"

# Tables to be processed
TABLES = {
    "order_items": "public.order_items",
    "orders": "public.orders",
    "products": "public.products",
    "order_reviews": "public.order_reviews",
    "product_category_name_translation": "public.product_category_name_translation"
}

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Path to SQL merge scripts
SQL_FOLDER = os.path.join(os.path.dirname(__file__), "SQL", "Merge")  # Path to Merge folder

# DAG definition
with DAG(
    "extract_load_postgres_to_gcs",  # DAG name remains the same
    default_args=default_args,
    description="ETL: Postgres -> GCS -> Stage -> Landing using merge SQL scripts",
    schedule_interval="@daily",  # Run daily
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=1,
) as dag:

    for table_name, pg_table in TABLES.items():

        # --- Step 1: Extract data from Postgres to GCS ---
        extract_to_gcs = PostgresToGCSOperator(
            task_id=f"extract_{table_name}_to_gcs",
            postgres_conn_id="postgres_db_abdelrahman",  # Postgres connection ID
            sql=f"SELECT * FROM {pg_table} WHERE updated_at_timestamp::date = '{{{{ ds }}}}'",
            bucket=BUCKET,
            filename=f"abdelrahman_db1/{table_name}/dt={{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/data.json",  # GCS folder path updated
            export_format="json",  # Exporting as JSON
            gzip=False,
        )

        # --- Step 2: Load data from GCS to staging in BigQuery ---
        load_to_stage = GCSToBigQueryOperator(
            task_id=f"load_{table_name}_to_stage",
            bucket=BUCKET,
            source_objects=[f"abdelrahman_db1/{table_name}/dt={{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/data.json"],  # GCS path updated
            destination_project_dataset_table=f"{PROJECT_ID}.{BQ_STAGE_DATASET}.{table_name}_stage_abdelrahman",  # BigQuery suffix updated
            source_format="NEWLINE_DELIMITED_JSON",  # Assuming we're uploading JSON data
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        # --- Step 3: Read merge SQL script ---
        sql_file_path = os.path.join(SQL_FOLDER, f"{table_name}_merge.sql")  # Correct path to SQL file
        with open(sql_file_path, "r") as f:
            merge_sql = f.read()

        # --- Step 4: Merge data from staging to landing in BigQuery ---
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

        # --- Step 5: Set task dependencies ---
        extract_to_gcs >> load_to_stage >> merge_to_landing
