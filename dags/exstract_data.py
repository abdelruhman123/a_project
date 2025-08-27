from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import storage
import pandas as pd
from datetime import datetime, timedelta
import json

# Helper Functions

def extract_data_from_postgres(conn_id, query, last_loaded_timestamp):
    """
    Extract data from PostgreSQL using incremental loading
    """
    # Create a Postgres hook to connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    sql = query.format(last_loaded_timestamp=last_loaded_timestamp)
    
    # Extract data as pandas DataFrame
    df = pg_hook.get_pandas_df(sql)
    return df

def upload_to_gcs(bucket_name, folder_name, file_name, data):
    """
    Upload extracted data to Google Cloud Storage
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/{file_name}")
    
    # Upload data as JSON
    blob.upload_from_string(data, content_type='application/json')
    print(f"Data uploaded to gs://{bucket_name}/{folder_name}/{file_name}")

def convert_df_to_json(df):
    """
    Convert pandas DataFrame to JSON format
    """
    return df.to_json(orient='records', lines=True)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 23),  # Start date for DAG
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'extract_load_postgres_to_gcs',
    default_args=default_args,
    description='A simple ELT pipeline to extract data from Postgres and upload to GCS',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
)

# Function to extract and upload data from DB1 (orders_products_db)
def extract_and_upload_db1_data():
    # Define the incremental loading timestamp (you may store and update it dynamically)
    last_loaded_timestamp = '2025-08-23'  # Placeholder, ideally should come from a record in BigQuery or DB
    
    # Define queries to extract data (replace with actual queries)
    query_orders = """
    SELECT * FROM public.orders
    WHERE updated_at_timestamp > '{last_loaded_timestamp}'
    """
    query_order_items = """
    SELECT * FROM public.order_items
    WHERE updated_at_timestamp > '{last_loaded_timestamp}'
    """
    
    # Extract data from DB1
    orders_df = extract_data_from_postgres('postgres_conn_db1', query_orders, last_loaded_timestamp)
    order_items_df = extract_data_from_postgres('postgres_conn_db1', query_order_items, last_loaded_timestamp)
    
    # Convert to JSON format
    orders_json = convert_df_to_json(orders_df)
    order_items_json = convert_df_to_json(order_items_df)
    
    # Upload data to GCS (for DB1)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db1', 'orders_abdelrahman.json', orders_json)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db1', 'order_items_abdelrahman.json', order_items_json)

# Function to extract and upload data from DB2 (customers_db)
def extract_and_upload_db2_data():
    last_loaded_timestamp = '2025-08-23'  # Placeholder
    
    # Define queries to extract data (replace with actual queries)
    query_customers = """
    SELECT * FROM public.customers
    WHERE updated_at_timestamp > '{last_loaded_timestamp}'
    """
    
    query_geolocation = """
    SELECT * FROM public.geolocation
    WHERE updated_at_timestamp > '{last_loaded_timestamp}'
    """
    
    # Extract data from DB2
    customers_df = extract_data_from_postgres('postgres_conn_db2', query_customers, last_loaded_timestamp)
    geolocation_df = extract_data_from_postgres('postgres_conn_db2', query_geolocation, last_loaded_timestamp)
    
    # Convert to JSON format
    customers_json = convert_df_to_json(customers_df)
    geolocation_json = convert_df_to_json(geolocation_df)
    
    # Upload data to GCS (for DB2)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db2', 'customers_abdelrahman.json', customers_json)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db2', 'geolocation_abdelrahman.json', geolocation_json)

# Define the Airflow tasks
task_extract_db1 = PythonOperator(
    task_id='extract_and_upload_db1_data',
    python_callable=extract_and_upload_db1_data,
    dag=dag
)

task_extract_db2 = PythonOperator(
    task_id='extract_and_upload_db2_data',
    python_callable=extract_and_upload_db2_data,
    dag=dag
)

# Set task dependencies
task_extract_db1 >> task_extract_db2  # Run task for DB1 first, then DB2
