import pandas as pd
import os
import logging

def clean_data(date_str: str) -> None:
    raw_path = f"data/raw/{date_str}/"
    clean_path = f"data/temp_clean/{date_str}/"
    os.makedirs(clean_path, exist_ok=True)

    for filename in os.listdir(raw_path):
        if filename.endswith(".csv"):
            try:
                df = pd.read_csv(os.path.join(raw_path, filename))
                
                df.dropna(inplace=True) #drops every raws that has any missing values
                df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
                df['vent'] = pd.to_numeric(df['vent'], errors='coerce') #coerce transform the missing value into NaN instead of raising an error

                df.to_csv(os.path.join(clean_path, filename), index=False)
                logging.info(f"Cleaned: {filename}")
            except Exception as e:
                logging.error(f"Error cleaning {filename}: {e}")