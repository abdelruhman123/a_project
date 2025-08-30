import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ========= Config =========

POSTGRES_CONN_DB2 = "postgres_abdelrahman_db2" 

GCS_BUCKET = "ready-labs-postgres-to-gcs"
GCS_PREFIX = "abdelrahman_db2"

BQ_PROJECT = "ready-de26"
BQ_DATASET = "project_landing"
BQ_FULL_DATASET = f"{BQ_PROJECT}.{BQ_DATASET}"
BQ_LOCATION = "US"

TABLES_DB2 = [
    "customers",
    "geolocation",
    "leads_closed",
    "leads_qualified",
]

TIMESTAMP_COLUMN = None   

SQL_FOLDER = os.path.join(os.path.dirname(__file__), "sql", "merge")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="abdelrahman_db2_parquet_pg_to_bq_with_merge",
    default_args=default_args,
    description="DB2: Postgres -> GCS (Parquet) -> BQ Staging (WRITE_TRUNCATE) -> MERGE to Landing",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 23),
    end_date=datetime(2025, 8, 27),
    catchup=True,
    max_active_runs=1,
    tags=["abdelrahman", "db2", "parquet", "staging", "landing"],
    is_paused_upon_creation=True,  
) as dag:

    start_pipeline = DummyOperator(task_id="start_pipeline")
    end_pipeline = DummyOperator(task_id="end_pipeline")

    for tbl in TABLES_DB2:
        # 1) Extract → GCS (Parquet)
        if TIMESTAMP_COLUMN:
            sql_query = f"""
                SELECT *
                FROM {tbl}
                WHERE {TIMESTAMP_COLUMN} >= '{{{{ macros.ds_add(ds, -1) }}}}'
                  AND {TIMESTAMP_COLUMN} <  '{{{{ ds }}}}'
            """
            parquet_name = "data.parquet"
        else:
            sql_query = f"SELECT * FROM {tbl}"
            parquet_name = "full.parquet"

        extract_to_gcs = PostgresToGCSOperator(
            task_id=f"abdelrahman_extract_db2_{tbl}_to_gcs_parquet",
            postgres_conn_id=POSTGRES_CONN_DB2,
            sql=sql_query,
            bucket=GCS_BUCKET,
            filename=(
                f"{GCS_PREFIX}/{tbl}/"
                f"dt={{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/{parquet_name}"
            ),
            export_format="parquet",
            gzip=False,
        )

        # 2) GCS → BigQuery (staging)
        load_to_staging = GCSToBigQueryOperator(
            task_id=f"abdelrahman_load_db2_{tbl}_to_staging_bq",
            bucket=GCS_BUCKET,
            source_objects=[
                f"{GCS_PREFIX}/{tbl}/dt={{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/{parquet_name}"
            ],
            destination_project_dataset_table=f"{BQ_FULL_DATASET}.{tbl}_stage_abdelrahman",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            autodetect=True,
            location=BQ_LOCATION,
        )

        # 3) Create landing if missing
        create_landing_if_missing = BigQueryInsertJobOperator(
            task_id=f"abdelrahman_create_db2_{tbl}_landing_if_missing",
            configuration={
                "query": {
                    "query": f"""
                        CREATE SCHEMA IF NOT EXISTS `{BQ_FULL_DATASET}`;
                        CREATE TABLE IF NOT EXISTS `{BQ_FULL_DATASET}`.`{tbl}_landing_abdelrahman`
                        AS SELECT * FROM `{BQ_FULL_DATASET}`.`{tbl}_stage_abdelrahman`
                        WHERE 1=0;
                    """,
                    "useLegacySql": False,
                }
            },
            location=BQ_LOCATION,
        )

        # 4) MERGE from file
        sql_file_path = os.path.join(SQL_FOLDER, f"{tbl}_merge.sql")
        with open(sql_file_path, "r") as f:
            merge_sql = f.read()

        merge_to_landing = BigQueryInsertJobOperator(
            task_id=f"abdelrahman_merge_db2_{tbl}_staging_to_landing",
            configuration={
                "query": {
                    "query": merge_sql,
                    "useLegacySql": False,
                }
            },
            location=BQ_LOCATION,
        )

        start_pipeline >> extract_to_gcs >> load_to_staging >> create_landing_if_missing >> merge_to_landing >> end_pipeline
