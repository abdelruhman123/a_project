# Extract order_payments API -> GCS as CSV
# Path: gs://ready-labs-postgres-to-gcs/abdelrahmanem-data/payments.csv

import json, requests, pendulum, pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from google.cloud import storage

BUCKET = "ready-labs-postgres-to-gcs"
FOLDER = "abdelrahmanem-data"
API_URL = "https://payments-table-834721874829.europe-west1.run.app"

@dag(
    dag_id="extract_order_payments_csv_to_gcs",
    schedule="@daily", start_date=pendulum.datetime(2025,8,1,tz="UTC"),
    catchup=False, default_args={"retries": 1}, tags=["api","csv","gcs"]
)
def dag_extract_payments():
    start = EmptyOperator(task_id="start")

    @task(task_id="fetch_and_upload_payments_csv", retries=2)
    def run():
        # 1) Fetch JSON
        r = requests.get(API_URL, timeout=60)
        r.raise_for_status()
        data = r.json()

        # 2) Normalize to DataFrame
        # handle { "data": [...] } or direct list
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        if not isinstance(data, list):
            data = [data]
        df = pd.json_normalize(data)

        # 3) Convert to CSV (UTF-8, comma)
        csv_bytes = df.to_csv(index=False).encode("utf-8")

        # 4) Upload to GCS (overwrite same object name)
        client = storage.Client()
        blob = client.bucket(BUCKET).blob(f"{FOLDER}/payments.csv")
        blob.upload_from_string(csv_bytes, content_type="text/csv")

        return f"gs://{BUCKET}/{FOLDER}/payments.csv"

    end = EmptyOperator(task_id="end")
    start >> run() >> end

dag = dag_extract_payments()
