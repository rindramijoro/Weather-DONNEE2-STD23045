import pandas as pd
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
def transform_to_star() -> str:
   
    input_file = "data/processed/meteo_global.csv"  
    output_dir = "data/star_schema"               
    os.makedirs(output_dir, exist_ok=True)     
    
   
    meteo_data = pd.read_csv(input_file)
    
   
    dim_ville_path = f"{output_dir}/dim_ville.csv"
    
   
    if os.path.exists(dim_ville_path):
        dim_ville = pd.read_csv(dim_ville_path)
    else:
        dim_ville = pd.DataFrame(columns=['ville_id', 'ville'])
    
   
    villes_existantes = set(dim_ville['ville'])
    nouvelles_villes = set(meteo_data['ville']) - villes_existantes
    
   
    if nouvelles_villes:
        nouveau_id = dim_ville['ville_id'].max() + 1 if not dim_ville.empty else 1
        nouvelles_lignes = pd.DataFrame({
            'ville_id': range(nouveau_id, nouveau_id + len(nouvelles_villes)),
            'ville': list(nouvelles_villes)
        })
        dim_ville = pd.concat([dim_ville, nouvelles_lignes], ignore_index=True)
        dim_ville.to_csv(dim_ville_path, index=False)  
    
   
    faits_meteo = meteo_data.merge(
        dim_ville,
        on='ville',
        how='left'
    ).drop(columns=['ville'])
    
    
    faits_path = f"{output_dir}/fact_weather.csv"
    faits_meteo.to_csv(faits_path, index=False)
    
    return faits_path