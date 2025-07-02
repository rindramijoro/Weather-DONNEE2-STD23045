import pandas as pd
import os
import logging

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

def clean_data(date_str: str) -> None:
    raw_path = os.path.join(DATA_DIR, "raw", date_str)
    clean_path = os.path.join(DATA_DIR, "temp_clean", date_str)

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw data folder not found: {raw_path}")

    os.makedirs(clean_path, exist_ok=True)

    for filename in os.listdir(raw_path):
        if filename.endswith(".csv"):
            try:
                df = pd.read_csv(os.path.join(raw_path, filename))
                
                df.dropna(inplace=True)  # Drop rows with missing values
                df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
                df['vent'] = pd.to_numeric(df['vent'], errors='coerce')  # Coerce invalid to NaN

                df.to_csv(os.path.join(clean_path, filename), index=False)
                logging.info(f"Cleaned: {filename}")
            except Exception as e:
                logging.error(f"Error cleaning {filename}: {e}")