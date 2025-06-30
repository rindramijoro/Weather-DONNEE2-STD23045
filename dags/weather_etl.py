from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from weather_pipeline.scripts.extract import extract_meteo
from weather_pipeline.scripts.clean import clean_data
from weather_pipeline.scripts.save import save_data
from weather_pipeline.scripts.merge import merge_files
from weather_pipeline.scripts.transform import transform_to_star

default_args = {
    'owner': 'Rindra Mijoro',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 29),
}

CITIES = ['Paris','London', 'Tokyo','New York','Antananarivo ']

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