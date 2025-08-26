@task
def extract_to_gcs(ds: str):
    hook = PostgresHook(postgres_conn_id="pg2")
    sql = """
      SELECT * FROM public.customers
      WHERE updated_at_timestamp >= (DATE %s) - INTERVAL '1 day'
        AND updated_at_timestamp <  (DATE %s) + INTERVAL '1 day';
    """
    try:
        df = hook.get_pandas_df(sql, parameters=(ds, ds))
        print(f"Fetched {len(df)} rows for {ds}")
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise e
    
    try:
        client = storage.Client(project="ready-de26")
        path = f"{BASE}/{ds}/customers.csv"
        client.bucket(BUCKET).blob(path).upload_from_string(
            df.to_csv(index=False), content_type="text/csv"
        )
        print(f"File uploaded successfully to: gs://{BUCKET}/{path}")
    except Exception as e:
        print(f"Error uploading file to GCS: {e}")
        raise e

    return f"gs://{BUCKET}/{path}"

