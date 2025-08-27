import requests
import pandas as pd
from google.cloud import storage
import json

# Set up GCS client using default application credentials (no service account file needed)
def upload_to_gcs(bucket_name, folder_name, file_name, data):
    # Create a GCS client using default credentials
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket(bucket_name)
    
    # Create the GCS path (folder in the bucket)
    blob = bucket.blob(f'{folder_name}/{file_name}')

    # Upload the data to GCS (assuming the data is in JSON format)
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

# Function to convert JSON data to CSV (optional, depending on your storage format)
def json_to_csv(data):
    # Convert the JSON data to a pandas DataFrame
    df = pd.DataFrame(data)
    
    # Convert the DataFrame to CSV
    csv_data = df.to_csv(index=False)
    return csv_data

# Main function to fetch from both APIs and upload to GCS
def extract_and_upload_data():
    # API endpoints
    payment_url = "https://payments-table-834721874829.europe-west1.run.app"
    seller_url = "https://sellers-table-834721874829.europe-west1.run.app"

    # Fetch data from the APIs
    payment_data = fetch_data_from_api(payment_url)
    seller_data = fetch_data_from_api(seller_url)

    if payment_data is not None:
        payment_csv = json_to_csv(payment_data)
        upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahmanem-data', 'payments_data.json', json.dumps(payment_data))
    
    if seller_data is not None:
        seller_csv = json_to_csv(seller_data)
        upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahmanem-data', 'sellers_data.json', json.dumps(seller_data))

# Run the extraction and upload process
extract_and_upload_data()
