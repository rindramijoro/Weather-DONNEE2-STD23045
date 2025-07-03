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

                # Drop rows with any missing values
                df.dropna(inplace=True)

                # Convert all relevant fields to numeric
                for col in ['temp_moyenne', 'temp_min', 'temp_max', 'precipitation_total', 'vent_moyen', 'jours_pluvieux']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                df.fillna(0, inplace=True)

                df.to_csv(os.path.join(clean_path, filename), index=False)
                logging.info(f"Cleaned: {filename}")
            except Exception as e:
                logging.error(f"Error cleaning {filename}: {e}")
