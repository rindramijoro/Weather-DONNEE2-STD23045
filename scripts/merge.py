import pandas as pd
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

FINAL_COLUMNS = [
    "ville", "date", "temperature", "temperature_min", "temperature_max",
    "precipitation", "vent", "pluie"
]

def merge_files(date: str) -> str:
    input_dir = os.path.join(DATA_DIR, "raw", date)
    output_file = os.path.join(DATA_DIR, "processed", "meteo_global.csv")
    historical_file = os.path.join(DATA_DIR, "raw", "historical_data.csv")

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Load previous meteo_global.csv if exists
    if os.path.exists(output_file):
        global_df = pd.read_csv(output_file)
    else:
        global_df = pd.DataFrame(columns=FINAL_COLUMNS)

    # Load new data from input_dir
    new_data = []
    for file in os.listdir(input_dir):
        if file.startswith('meteo_') and file.endswith('.csv'):
            new_df = pd.read_csv(os.path.join(input_dir, file))
            new_data.append(new_df)
    if not new_data:
        raise ValueError(f"Aucune nouvelle donnée à fusionner pour {date}")
    extracted_df = pd.concat(new_data, ignore_index=True)

    # Load and transform historical data
    if os.path.exists(historical_file):
        historical_df = pd.read_csv(historical_file)

        # Rename columns
        historical_df = historical_df.rename(columns={
            'temp_moyenne': 'temperature',
            'temp_min': 'temperature_min',
            'temp_max': 'temperature_max',
            'precipitation_totale': 'precipitation',
            'vent_moyen': 'vent',
            'jours_pluvieux': 'pluie'
        })

        # Keep only final columns
        historical_df = historical_df[FINAL_COLUMNS]
    else:
        historical_df = pd.DataFrame(columns=FINAL_COLUMNS)

    # Ensure extracted and global data use final columns too
    for df in [extracted_df, global_df]:
        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = 0.0
        df.drop(columns=[c for c in df.columns if c not in FINAL_COLUMNS], inplace=True)

    # Merge all
    updated_df = pd.concat([historical_df, global_df, extracted_df], ignore_index=True)

    # Drop duplicates by ville + date
    updated_df = updated_df.drop_duplicates(subset=['ville', 'date'], keep='last')

    updated_df.to_csv(output_file, index=False)
    return output_file
