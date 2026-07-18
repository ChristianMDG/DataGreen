from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'catchup': False,
}

dag = DAG(
    'air_quality_pipeline',
    default_args=default_args,
    description='Pipeline qualite de l air',
    schedule_interval='@hourly',
    tags=['air_quality'],
    max_active_runs=1,
)

def extract_data():
    print("Extraction des donnees...")
    return "OK"

def transform_data():
    print("Transformation des donnees...")
    return "OK"

def load_data():
    print("Chargement des donnees...")
    return "OK"

start = DummyOperator(task_id='start', dag=dag)
extract = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
transform = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
load = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> extract >> transform >> load >> end
