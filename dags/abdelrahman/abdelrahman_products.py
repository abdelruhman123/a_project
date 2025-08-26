import pendulum, pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import storage

BUCKET = "ready-labs-postgres-to-gcs"
BASE   = "abdelrahman/products"

@dag(
    dag_id="abdelrahman_products_to_gcs",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False, tags=["pg","gcs","incremental"]
)
def _dag():
    @task
    def extract_to_gcs(ds: str):
        hook = PostgresHook(postgres_conn_id="pg1")
        sql = """
          SELECT * FROM public.products
          WHERE updated_at_timestamp >= (DATE %s) - INTERVAL '1 day'
            AND updated_at_timestamp <  (DATE %s) + INTERVAL '1 day';
        """
        print(f"Running query for date: {ds}")
        try:
            df = hook.get_pandas_df(sql, parameters=(ds, ds))
            print(f"Fetched {len(df)} rows")
        except Exception as e:
            print(f"Error fetching data: {e}")
            raise e
        
        try:
            client = storage.Client(project="ready-de26")
            path = f"{BASE}/{ds}/products.csv"
            client.bucket(BUCKET).blob(path).upload_from_string(
                df.to_csv(index=False), content_type="text/csv"
            )
            print(f"File uploaded to: gs://{BUCKET}/{path}")
        except Exception as e:
            print(f"Error uploading file to GCS: {e}")
            raise e

        return f"gs://{BUCKET}/{path}"
    extract_to_gcs()
dag = _dag()
