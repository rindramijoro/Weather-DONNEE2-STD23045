import os
import pandas as pd
import logging

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def save_data(date_str: str) -> None:
    temp_clean_path = f"data/temp_clean/{date_str}/"
    final_clean_path = f"data/clean/{date_str}/"
    os.makedirs(final_clean_path, exist_ok=True)

    for filename in os.listdir(temp_clean_path):
        if filename.endswith(".csv"):
            try:
                df = pd.read_csv(os.path.join(temp_clean_path, filename))
                
                # final formatting before saving
                df.to_csv(os.path.join(final_clean_path, filename), index=False)
                logging.info(f"Saved cleaned file: {filename}")
            except Exception as e:
                logging.error(f"Error saving {filename}: {e}")