
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

BUCKET = "ready-labs-postgres-to-gcs"
DST    = "abdelrahmanem-data/sellers.csv"
API_URL = "https://sellers-table-834721874829.europe-west1.run.app"
TMP    = "/tmp/sellers.csv"

@dag(
    dag_id="extract_sellers_csv_to_gcs",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False, tags=["api","csv","gcs"]
)
def dag_extract_sellers():
    start = EmptyOperator(task_id="start")

    @task(task_id="fetch_to_tmp_csv", retries=2)
    def fetch_to_tmp_csv():
        import requests, pandas as pd
        r = requests.get(API_URL, timeout=60); r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "data" in data: data = data["data"]
        if not isinstance(data, list): data = [data]
        pd.json_normalize(data).to_csv(TMP, index=False)
        print(f"[INFO] wrote {TMP}")

    upload = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=TMP,
        dst=DST,
        bucket=BUCKET,
        gcp_conn_id="google_cloud_default",
        mime_type="text/csv",
        gzip=False,
    )

    end = EmptyOperator(task_id="end")
    start >> fetch_to_tmp_csv() >> upload >> end

dag = dag_extract_sellers()
