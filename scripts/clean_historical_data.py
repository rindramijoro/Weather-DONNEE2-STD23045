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

        for col in ['temp_moyenne', 'temp_min', 'temp_max']:
            df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Clean numeric columns that may already be clean
        for col in ['precipitation_totale', 'vent_moyen', 'jours_pluvieux']:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Fill missing values with 0
        df.fillna(0, inplace=True)

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False)
        logging.info(f"Cleaned historical data saved to {output_file}")

    except Exception as e:
        logging.error(f"Error cleaning historical data: {e}")
