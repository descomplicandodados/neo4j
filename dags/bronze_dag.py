import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Adiciona o diretório scripts ao Python path
sys.path.insert(0, '/opt/airflow/scripts')

# Agora importa diretamente
from load_raw_neo4j import load_raw_files

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'bronze_layer_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    load_raw_task = PythonOperator(
        task_id='load_raw_files',
        python_callable=load_raw_files
    )