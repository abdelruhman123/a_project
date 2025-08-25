import requests
import pandas as pd

def call_api(url: str) -> pd.DataFrame:
    """
    Function to call an API endpoint and return data as a pandas DataFrame
    """
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an error for bad status codes
        data = response.json()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error calling API {url}: {e}")
        return pd.DataFrame()
