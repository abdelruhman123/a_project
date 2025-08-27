import psycopg2
import pandas as pd
from google.cloud import storage
import json

# Function to connect to PostgreSQL
def connect_to_postgres(host, user, password, port, dbname):
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        port=port,
        dbname=dbname
    )
    return conn

# Function to fetch data from a table based on incremental loading
def fetch_data_from_table(conn, table_name, last_loaded_timestamp):
    query = f"""
        SELECT * 
        FROM {table_name}
        WHERE updated_at_timestamp > '{last_loaded_timestamp}'
    """
    df = pd.read_sql(query, conn)
    return df

# Function to upload data to Google Cloud Storage
def upload_to_gcs(bucket_name, folder_name, file_name, data):
    # Create a GCS client using default application credentials
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket(bucket_name)

    # Create the GCS path (folder in the bucket)
    blob = bucket.blob(f'{folder_name}/{file_name}')

    # Upload the data to GCS (assuming the data is in JSON format)
    blob.upload_from_string(data, content_type='application/json')
    print(f"Data uploaded to gs://{bucket_name}/{folder_name}/{file_name}")

# Function to convert DataFrame data into JSON format
def df_to_json(df):
    return df.to_json(orient='records', lines=True)

# Main function to extract data and upload to GCS
def extract_and_upload_data():
    # DB1 (orders_products_db) connection details
    db1_conn = connect_to_postgres('34.173.180.170', 'postgres', 'Ready-de26', 5432, 'postgres')
    
    # DB2 (customers_db) connection details
    db2_conn = connect_to_postgres('34.66.139.70', 'postgres', 'Ready-de26', 5432, 'postgres')

    # Define the last loaded timestamp (this will be dynamically updated each time the data is processed)
    last_loaded_timestamp = '2025-08-23'  # Example timestamp, update as needed.

    # Fetch data from DB1 tables (orders_products_db)
    order_items_data = fetch_data_from_table(db1_conn, 'public.order_items', last_loaded_timestamp)
    order_reviews_data = fetch_data_from_table(db1_conn, 'public.order_reviews', last_loaded_timestamp)
    orders_data = fetch_data_from_table(db1_conn, 'public.orders', last_loaded_timestamp)
    products_data = fetch_data_from_table(db1_conn, 'public.products', last_loaded_timestamp)
    product_category_name_translation_data = fetch_data_from_table(db1_conn, 'public.product_category_name_translation', last_loaded_timestamp)

    # Fetch data from DB2 tables (customers_db)
    customers_data = fetch_data_from_table(db2_conn, 'public.customers', last_loaded_timestamp)
    geolocation_data = fetch_data_from_table(db2_conn, 'public.geolocation', last_loaded_timestamp)

    # Convert the fetched data to JSON format
    order_items_json = df_to_json(order_items_data)
    order_reviews_json = df_to_json(order_reviews_data)
    orders_json = df_to_json(orders_data)
    products_json = df_to_json(products_data)
    product_category_name_translation_json = df_to_json(product_category_name_translation_data)

    customers_json = df_to_json(customers_data)
    geolocation_json = df_to_json(geolocation_data)

    # Upload data to GCS (using the defined folder names)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db1', 'order_items_abdelrahman.json', order_items_json)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db1', 'order_reviews_abdelrahman.json', order_reviews_json)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db1', 'orders_abdelrahman.json', orders_json)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db1', 'products_abdelrahman.json', products_json)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db1', 'product_category_name_translation_abdelrahman.json', product_category_name_translation_json)

    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db2', 'customers_abdelrahman.json', customers_json)
    upload_to_gcs('ready-labs-postgres-to-gcs', 'abdelrahman_db2', 'geolocation_abdelrahman.json', geolocation_json)

    # Close database connections
    db1_conn.close()
    db2_conn.close()

# Run the extraction and upload process
extract_and_upload_data()
