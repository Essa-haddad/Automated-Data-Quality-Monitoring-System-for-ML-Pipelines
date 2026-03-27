import os
import json
import pandas as pd


# Load csv file into pandas DataFrame
def load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")

    return pd.read_csv(path)


# Load JSON file into dictionary
def load_json(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"JSON file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# Returns list of CSV files in a folder alphabetically
def get_csv_files(folder_path: str) -> list:
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    csv_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith(".csv")]

    return sorted(csv_files)


# Create a directory if it does not already exist
def ensure_directory(path: str) -> None:
    os.makedirs(path, exist_ok=True)
