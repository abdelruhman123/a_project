import pendulum, pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import storage

BUCKET = "ready-labs-postgres-to-gcs"
BASE   = "abdelrahman/orders"   # << بدلنا المسار

@dag(
    dag_id="abdelrahman_orders_to_gcs",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False, tags=["pg","gcs","incremental"]
)
def _dag():
    @task
    def extract_to_gcs(ds: str):
        hook = PostgresHook(postgres_conn_id="pg1")
        sql = """
          SELECT * FROM public.orders
          WHERE updated_at_timestamp >= (DATE %s) - INTERVAL '1 day'
            AND updated_at_timestamp <  (DATE %s) + INTERVAL '1 day';
        """
        df = hook.get_pandas_df(sql, parameters=(ds, ds))
        client = storage.Client(project="ready-de26")
        path = f"{BASE}/{ds}/orders.csv"
        client.bucket(BUCKET).blob(path).upload_from_string(
            df.to_csv(index=False), content_type="text/csv"
        )
        return f"gs://{BUCKET}/{path}"
    extract_to_gcs()
dag = _dag()
