import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

sys.path.insert(0, '/opt/airflow/scripts')

from load_silver_neo4j import load_silver

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='silver_layer_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['neo4j', 'silver']
) as dag:

    silver_task = PythonOperator(
        task_id='load_silver_neo4j',
        python_callable=load_silver
    )


