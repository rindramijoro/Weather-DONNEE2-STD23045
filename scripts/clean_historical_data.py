import pandas as pd
import os
import logging

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

def clean_historical_data() -> None:
    input_file = os.path.join(DATA_DIR, "historical_data", "historical_data.csv")
    output_file = os.path.join(DATA_DIR, "raw", "historical_data.csv")

    try:
        df = pd.read_csv(input_file)

        # Rename columns to match current format
        df.rename(columns={
            'temp_moyenne': 'temperature',
            'temp_min': 'temperature_min',
            'temp_max': 'temperature_max',
            'precipitation_totale': 'precipitation',
            'vent_moyen': 'vent',
            'jours_pluvieux': 'pluie'
        }, inplace=True)

        for col in ['temperature', 'temperature_min', 'temperature_max']:
            df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in ['precipitation', 'vent', 'pluie']:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df.fillna(0, inplace=True)

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False)
        logging.info(f"Cleaned historical data saved to {output_file}")

    except Exception as e:
        logging.error(f"Error cleaning historical data: {e}")
