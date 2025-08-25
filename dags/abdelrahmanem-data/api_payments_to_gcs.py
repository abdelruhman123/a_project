from airflow.decorators import dag, task
from datetime import datetime
from include.helpers.helpers import call_api
from include.schemas import ORDER_PAYMENTS_SCHEMA
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os, json, pandas as pd

# Local staging
RAW_DIR = "/usr/local/airflow/include/output/raw"
BQREADY_DIR = "/usr/local/airflow/include/output/bq_ready"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(BQREADY_DIR, exist_ok=True)

# GCS info
BUCKET = "ready-labs-postgres-to-gcs"
FOLDER = "abdelrahmanem-data/"

# API
PAYMENTS_URL = "https://payments-table-834721874829.europe-west1.run.app"

def _schema_to_pandas_dtypes(schema):
    mapping = {"STRING": "string", "INTEGER": "Int64", "FLOAT": "float64"}
    return {c["name"]: mapping.get(c["type"], "string") for c in schema}

def _enforce_schema(df: pd.DataFrame, schema):
    df.columns = [c.strip().lower() for c in df.columns]
    for col in [c["name"] for c in schema]:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[[c["name"] for c in schema]]
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
    with open(path, "w") as f:
        json.dump(schema, f, indent=2)

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["payments","api","gcs"]
)
def payments_api_to_gcs():

    @task()
    def extract() -> str:
        data = call_api(PAYMENTS_URL)
        out_dir = os.path.join(RAW_DIR, "order_payments", "{{ ds }}")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "order_payments.json")
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    @task()
    def clean_and_type(raw_path: str) -> dict:
        schema = ORDER_PAYMENTS_SCHEMA
        df = pd.read_json(raw_path)
        assert df["order_id"].notna().all(), "order_id has NULL values"
        df = _enforce_schema(df, schema)
        out_dir = os.path.join(BQREADY_DIR, "order_payments", "{{ ds }}")
        os.makedirs(out_dir, exist_ok=True)
        parquet_path = os.path.join(out_dir, "order_payments.parquet")
        df.to_parquet(parquet_path, index=False)
        schema_path = os.path.join(out_dir, "order_payments_schema.json")
        _save_bq_schema_json(schema, schema_path)
        return {"parquet": parquet_path, "schema": schema_path}

    def upload_to_gcs(local_path: str, dst_path: str, task_id: str):
        return LocalFilesystemToGCSOperator(
            task_id=task_id,
            src=local_path,
            dst=FOLDER + dst_path,
            bucket=BUCKET,
            gcp_conn_id="google_cloud_default"
        )

    raw = extract()
    ready = clean_and_type(raw)
    upload_to_gcs(ready["parquet"], "order_payments/order_payments.parquet", "upload_parquet")
    upload_to_gcs(ready["schema"], "order_payments/order_payments_schema.json", "upload_schema")

payments_api_to_gcs = payments_api_to_gcs()