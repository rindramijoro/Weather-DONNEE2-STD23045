import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.extract import extract_meteo
from scripts.clean import clean_data
from scripts.save import save_data
from scripts.merge import merge_files
from scripts.transform import transform_to_star
from scripts.clean_historical_data import clean_historical_data

default_args = {
    'owner': 'Rindra Mijoro',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 29),
}

CITIES = ['Sydney','New York','Paris','London', 'Tokyo']

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:
    extract_tasks = [
        PythonOperator(
            task_id=f'extract_{city.lower().replace(" ", "_")}',
            python_callable=extract_meteo,
            op_args=[city, "{{ var.value.API_KEY }}", "{{ ds }}"],
        )
        for city in CITIES
    ]

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        op_args=["{{ ds }}"], 
    )
    
    clean_history_task = PythonOperator(
        task_id='clean_historical_data',
        python_callable=clean_historical_data
    )

    save_data_task = PythonOperator(
        task_id='save_cleaned_data',
        python_callable=save_data,
        op_args=["{{ ds }}"],
    )

    merge_task = PythonOperator(
        task_id='merge_file',
        python_callable=merge_files,
        op_args=["{{ ds }}"],
    )

    transform_task = PythonOperator(
        task_id='transform_to_star',
        python_callable=transform_to_star
    )

    extract_tasks >> clean_data_task >> save_data_task >> merge_task >> transform_task