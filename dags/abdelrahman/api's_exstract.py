from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
import requests
import pandas as pd
import json
from datetime import datetime, timedelta

# Function to upload data to Google Cloud Storage
def upload_to_gcs(bucket_name, folder_name, file_name, data):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{file_name}')
    blob.upload_from_string(data, content_type='application/json')
    print(f"Data uploaded to gs://{bucket_name}/{folder_name}/{file_name}")

# Function to fetch data from the API
def fetch_data_from_api(url):
    print(f"Fetching data from {url}")
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data from {url}")
        return None

# Function to convert JSON data to CSV (optional)
def json_to_csv(data):
    df = pd.DataFrame(data)
    csv_data = df.to_csv(index=False)
    return csv_data

# Main function to extract data and upload to GCS
def extract_and_upload_data():
    # API endpoints
    payment_url = "https://payments-table-834721874829.europe-west1.run.app"
    seller_url = "https://sellers-table-834721874829.europe-west1.run.app"

    # Fetch data from the APIs
    payment_data = fetch_data_from_api(payment_url)
    seller_data = fetch_data_from_api(seller_url)

    # Check if data was fetched successfully and upload it to GCs
    if payment_data is not None:
        payment_json = json.dumps(payment_data)
        upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahmanem-data', 'payments_data.json', payment_json)

    if seller_data is not None:
        seller_json = json.dumps(seller_data)
        upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahmanem-data', 'sellers_data.json', seller_json)

# Airflow Default Args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 23),  # Start date for DAG
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'extract_and_upload_api_data_to_gcs',
    default_args=default_args,
    description='Extract data from APIs and upload to GCS',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
)

# Define the PythonOperator tasks in the DAG
task_extract_and_upload_data = PythonOperator(
    task_id='extract_and_upload_data',
    python_callable=extract_and_upload_data,
    dag=dag
)

# Set task dependencies
task_extract_and_upload_data
