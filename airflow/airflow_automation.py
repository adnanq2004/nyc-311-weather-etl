# airflow/airflow_automation.py

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys
from etl.extraction.extract_311 import extract_311
from etl.extraction.extract_weather import extract_weather
from etl.transformation.transform_311 import transform_311
from etl.transformation.transform_combined import transform_combined
from etl.loading.load_to_bigquery import load_to_bigquery
from logger.etl_logger import ETLLogger


# Add ETL modules to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Set up logging
logger = ETLLogger("airflow_dag").get()
logger.info("Starting Airflow DAG script")

# Default arguments for DAG
default_args = {
    'owner': 'agxdata',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'nyc_311_weather_etl_monthly',
    default_args=default_args,
    description='Monthly ETL pipeline for NYC 311 and weather data',
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    tags=['nyc', '311', 'weather', 'etl']
) as dag:

    # Utility functions for logging
    def log_task_start(task_name):
        logger.info(f"Starting task: {task_name}")

    def log_task_end(task_name):
        logger.info(f"Finished task: {task_name}")

    # Wrapper functions for each ETL step
    def run_extract_311():
        log_task_start("extract_311")
        extract_311()
        log_task_end("extract_311")

    def run_extract_weather():
        log_task_start("extract_weather")
        extract_weather()
        log_task_end("extract_weather")

    def run_transform_311():
        log_task_start("transform_311")
        transform_311()
        log_task_end("transform_311")

    def run_transform_combined():
        log_task_start("transform_combined")
        transform_combined()
        log_task_end("transform_combined")

    def run_load_to_bigquery():
        log_task_start("load_to_bigquery")
        load_to_bigquery()
        log_task_end("load_to_bigquery")

    # Define tasks
    extract_311_task = PythonOperator(task_id='extract_311', python_callable=run_extract_311)
    extract_weather_task = PythonOperator(task_id='extract_weather', python_callable=run_extract_weather)
    transform_311_task = PythonOperator(task_id='transform_311', python_callable=run_transform_311)
    transform_combined_task = PythonOperator(task_id='transform_combined', python_callable=run_transform_combined)
    load_to_bigquery_task = PythonOperator(task_id='load_to_bigquery', python_callable=run_load_to_bigquery)

    # DAG dependencies
    [extract_311_task, extract_weather_task] >> transform_311_task
    transform_311_task >> transform_combined_task
    transform_combined_task >> load_to_bigquery_task