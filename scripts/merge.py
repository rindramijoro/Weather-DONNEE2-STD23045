import pandas as pd
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def merge_files(date: str) -> str:
    input_dir = f"data/raw/{date}"
    output_file = "data/processed/meteo_global.csv"
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    if os.path.exists(output_file):
        global_df = pd.read_csv(output_file)
    else:
        global_df = pd.DataFrame()
        
    new_data = []
    for file in os.listdir(input_dir):
        if file.startswith('meteo_') and file.endswith('.csv'):
            new_data.append(pd.read_csv(f"{input_dir}/{file}"))
            
    if not new_data:
        raise ValueError(f"Aucune nouvelle donnée à fusionner pour {date}")
    
    updated_df = pd.concat([global_df] + new_data, ignore_index=True)
    updated_df = updated_df.drop_duplicates(
        subset=['ville', 'date_extraction'],  
        keep='last'                          
    )
    
    updated_df.to_csv(output_file, index=False)
    return output_file