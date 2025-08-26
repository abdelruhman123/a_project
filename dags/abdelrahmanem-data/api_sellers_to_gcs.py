# Extract sellers API -> GCS as CSV
# Path: gs://ready-labs-postgres-to-gcs/abdelrahmanem-data/sellers.csv

import json, requests, pendulum, pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from google.cloud import storage

BUCKET = "ready-labs-postgres-to-gcs"
FOLDER = "abdelrahmanem-data"
API_URL = "https://sellers-table-834721874829.europe-west1.run.app"

@dag(
    dag_id="extract_sellers_csv_to_gcs",
    schedule="@daily", start_date=pendulum.datetime(2025,8,1,tz="UTC"),
    catchup=False, default_args={"retries": 1}, tags=["api","csv","gcs"]
)
def dag_extract_sellers():
    start = EmptyOperator(task_id="start")

    @task(task_id="fetch_and_upload_sellers_csv", retries=2)
    def run():
        r = requests.get(API_URL, timeout=60)
        r.raise_for_status()
        data = r.json()

        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        if not isinstance(data, list):
            data = [data]
        df = pd.json_normalize(data)

        csv_bytes = df.to_csv(index=False).encode("utf-8")

        from google.cloud import storage
        client = storage.Client()
        blob = client.bucket(BUCKET).blob(f"{FOLDER}/sellers.csv")
        blob.upload_from_string(csv_bytes, content_type="text/csv")

        return f"gs://{BUCKET}/{FOLDER}/sellers.csv"

    end = EmptyOperator(task_id="end")
    start >> run() >> end

dag = dag_extract_sellers()
