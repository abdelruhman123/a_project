from airflow.decorators import dag, task
from datetime import datetime
from include.helpers.helpers import call_api
from include.schemas import ORDER_PAYMENTS_SCHEMA, SELLERS_SCHEMA
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os, json, pandas as pd

# Local staging before upload
RAW_DIR = "/usr/local/airflow/include/output/raw"          # raw json
BQREADY_DIR = "/usr/local/airflow/include/output/bq_ready" # parquet + schema
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(BQREADY_DIR, exist_ok=True)

# GCS bucket
BUCKET = "your-bucket-name"  

# API endpoints
PAYMENTS_URL = "https://payments-table-834721874829.europe-west1.run.app"
SELLERS_URL  = "https://sellers-table-834721874829.europe-west1.run.app"

def _schema_to_pandas_dtypes(schema):
    """Map BigQuery schema types to pandas dtypes."""
    mapping = {"STRING": "string", "INTEGER": "Int64", "FLOAT": "float64"}
    return {c["name"]: mapping.get(c["type"], "string") for c in schema}

def _enforce_schema(df: pd.DataFrame, schema):
    """Normalize columns and enforce schema consistency."""
    df.columns = [c.strip().lower() for c in df.columns]

    # Add missing columns
    for col in [c["name"] for c in schema]:
        if col not in df.columns:
            df[col] = pd.NA

    # Reorder
    df = df[[c["name"] for c in schema]]

    # Cast types
    dtypes = _schema_to_pandas_dtypes(schema)
    for col, dt in dtypes.items():
        try:
            if dt == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dt == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            else:
                df[col] = df[col].astype("string")
        except Exception:
            df[col] = df[col].astype("string")
    return df

def _save_bq_schema_json(schema, path):
    """Save BigQuery schema as JSON file."""
    with open(path, "w") as f:
        json.dump(schema, f, indent=2)

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["api","gcs","bigquery"]
)
def api_to_gcs_pipeline():

    @task()
    def extract(endpoint: str, table_name: str) -> str:
        """Extract raw data from API and save as JSON locally."""
        data = call_api(endpoint)
        out_dir = os.path.join(RAW_DIR, table_name, "{{ ds }}")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{table_name}.json")
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    @task()
    def clean_and_type(raw_path: str, table_name: str, schema_name: str) -> dict:
        """Clean, enforce schema, and save as Parquet + Schema JSON."""
        schema = ORDER_PAYMENTS_SCHEMA if schema_name == "payments" else SELLERS_SCHEMA
        df = pd.read_json(raw_path)

        # Light quality checks
        if "order_id" in df.columns:
            assert df["order_id"].notna().all(), "order_id has NULL values"
        if "seller_id" in df.columns:
            assert df["seller_id"].notna().all(), "seller_id has NULL values"

        df = _enforce_schema(df, schema)

        # Save parquet
        out_dir = os.path.join(BQREADY_DIR, table_name, "{{ ds }}")
        os.makedirs(out_dir, exist_ok=True)
        parquet_path = os.path.join(out_dir, f"{table_name}.parquet")
        df.to_parquet(parquet_path, index=False)

        # Save schema
        schema_path = os.path.join(out_dir, f"{table_name}_schema.json")
        _save_bq_schema_json(schema, schema_path)

        return {"parquet": parquet_path, "schema": schema_path}

    def upload_to_gcs(local_path: str, dst_path: str, task_id: str):
        """Upload a local file to GCS."""
        return LocalFilesystemToGCSOperator(
            task_id=task_id,
            src=local_path,
            dst=dst_path,
            bucket=BUCKET,
            gcp_conn_id="google_cloud_default"
        )

    # Payments
    payments_raw = extract(PAYMENTS_URL, "order_payments")
    payments_ready = clean_and_type(payments_raw, "order_payments", "payments")
    upload_to_gcs(payments_ready["parquet"], "bq_ready/order_payments.parquet", "upload_payments_parquet")
    upload_to_gcs(payments_ready["schema"], "bq_ready/order_payments_schema.json", "upload_payments_schema")

    # Sellers
    sellers_raw = extract(SELLERS_URL, "sellers")
    sellers_ready = clean_and_type(sellers_raw, "sellers", "sellers")
    upload_to_gcs(sellers_ready["parquet"], "bq_ready/sellers.parquet", "upload_sellers_parquet")
    upload_to_gcs(sellers_ready["schema"], "bq_ready/sellers_schema.json", "upload_sellers_schema")

    # Uncomment when BigQuery is ready
    # load_payments_bq = GCSToBigQueryOperator(
    #     task_id="load_payments_bq",
    #     bucket=BUCKET,
    #     source_objects=["bq_ready/order_payments.parquet"],
    #     destination_project_dataset_table="your_project.your_dataset.order_payments",
    #     source_format="PARQUET",
    #     write_disposition="WRITE_APPEND"
    # )
    #
    # load_sellers_bq = GCSToBigQueryOperator(
    #     task_id="load_sellers_bq",
    #     bucket=BUCKET,
    #     source_objects=["bq_ready/sellers.parquet"],
    #     destination_project_dataset_table="your_project.your_dataset.sellers",
    #     source_format="PARQUET",
    #     write_disposition="WRITE_APPEND"
    # )

api_to_gcs_pipeline = api_to_gcs_pipeline()















