import os
import requests
import pandas as pd
from datetime import datetime
import logging

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def extract_meteo(city: str, api_key: str, date: str) -> bool:
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q' : city,
            'appid' : api_key,
            'units' : 'metric',
            'lang' : 'fr' 
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        weather_data = {
            'ville': city,
            'date_extraction': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'temperature': data['main']['temp'],
            'humidite': data['main']['humidity'],
            'pression': data['main']['pressure'],
            'vent': data['wind']['speed'],
            'description': data['weather'][0]['description']
        }

        # Use full path here
        save_dir = os.path.join(BASE_DIR, f"data/raw/{date}")
        os.makedirs(save_dir, exist_ok=True)

        csv_path = os.path.join(save_dir, f"meteo_{city}.csv")
        pd.DataFrame([weather_data]).to_csv(csv_path, index=False)

        logging.info(f"Saved CSV to: {csv_path}")
        return True
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {str(e)}")
        
    return False
