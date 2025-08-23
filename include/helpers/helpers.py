import requests
import json
import os

def call_api(url: str, params: dict = None, headers: dict = None):
    """
    Function to call an API and return JSON response.
    """
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()  # لو فيه error يوقف
    return response.json()


def save_json_to_file(data: dict, filepath: str):
    """
    Save JSON response locally to a file.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


def save_list_to_csv(data: list, filepath: str, fieldnames: list):
    """
    Save a list of dicts (API response) into CSV.
    """
    import csv
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
