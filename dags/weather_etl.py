from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from weather_pipeline.scripts.extract import extract_meteo
from weather_pipeline.scripts.merge import merge_files
from weather_pipeline.scripts.transform import transform_to_star

default_args = {
    'owner': 'Rindra Mijoro',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 29),
}

CITIES = ['Paris','Lodon', 'Tokyo','New York','Sydney']

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
) as dag:
    extract_tasks = [
        PythonOperator(
            task_id=f'extract_{city.lower().replace(" ", "_")}',
            python_callable=extract_meteo,
            op_args=[city, "{{var.value.API_KEY}}", "{{ ds }}"],
        )
        for city in CITIES
    ]
    
    merge_task = PythonOperator(
        task_id='merge_file',
        python_callable=merge_files,
        op_args=["{{ ds }}"],
    )
    
    transform_task = PythonOperator(
        task_id='transform_to_star',
        pythin_callable=transform_to_star
    )
    
    extract_tasks >> merge_task >> transform_task