import pandas as pd
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

def transform_to_star() -> str:
    input_file = os.path.join(DATA_DIR, "processed", "meteo_global.csv")
    output_dir = os.path.join(DATA_DIR, "star_schema")
    os.makedirs(output_dir, exist_ok=True)

    meteo_data = pd.read_csv(input_file)

    # Create dimension table for cities
    dim_ville_path = os.path.join(output_dir, "dim_ville.csv")

    if os.path.exists(dim_ville_path):
        dim_ville = pd.read_csv(dim_ville_path)
    else:
        dim_ville = pd.DataFrame(columns=["ville_id", "ville"])

    existing_villes = set(dim_ville["ville"])
    new_villes = set(meteo_data["ville"]) - existing_villes

    if new_villes:
        start_id = dim_ville["ville_id"].max() + 1 if not dim_ville.empty else 1
        new_rows = pd.DataFrame({
            "ville_id": range(start_id, start_id + len(new_villes)),
            "ville": list(new_villes)
        })
        dim_ville = pd.concat([dim_ville, new_rows], ignore_index=True)
        dim_ville.to_csv(dim_ville_path, index=False)

    # Create fact table
    fact_weather = meteo_data.merge(dim_ville, on="ville", how="left").drop(columns=["ville"])

    # Optional: reorder columns (ville_id first, then date, etc.)
    fact_weather = fact_weather[["ville_id", "date", "temp_moyenne", "temp_min", "temp_max",
                                 "precipitation_totale", "vent_moyen", "jours_pluvieux"]]

    facts_path = os.path.join(output_dir, "fact_weather.csv")
    fact_weather.to_csv(facts_path, index=False)

    return facts_path
