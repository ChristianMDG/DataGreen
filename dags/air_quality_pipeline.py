"""
DAG Airflow - Pipeline de Qualité de l'Air
Extraction horaire depuis OpenWeather API - 5 villes européennes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import logging
import sys
import os

# Ajouter le chemin des scripts
sys.path.append('/opt/airflow/scripts')

# Importer les fonctions des scripts
from extract import extract_air_quality_data
from transform import transform_air_quality_data
from load_warehouse import load_to_warehouse
from validate_clean import validate_clean_file

logger = logging.getLogger(__name__)

# ==============================================
# CONFIGURATION
# ==============================================

DEFAULT_ARGS = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'execution_timeout': timedelta(minutes=30),
}

# Récupérer les variables Airflow
CITIES = Variable.get("cities", 
                      default_var='["Paris","London","Berlin","Madrid","Rome"]', 
                      deserialize_json=True)

API_KEY = Variable.get("openweather_api_key", default_var=None)

RAW_PATH = Variable.get("raw_path", default_var="/opt/airflow/data/raw")
CLEAN_PATH = Variable.get("clean_path", default_var="/opt/airflow/data/clean")

# ==============================================
# DÉFINITION DU DAG
# ==============================================

dag = DAG(
    'air_quality_pipeline',
    default_args=DEFAULT_ARGS,
    description='Pipeline ETL qualité de l\'air - 5 villes européennes',
    schedule_interval='@hourly',
    tags=['air_quality', 'etl', 'iai'],
    max_active_runs=1,
    doc_md="""
    ### Pipeline Qualité de l'Air
    
    **Objectif** : Collecter automatiquement les données de qualité de l'air
    
    **Villes** : Paris, London, Berlin, Madrid, Rome
    
    **Flux** :
    1. Extraction horaire depuis OpenWeather API
    2. Stockage raw/ (JSON intouchables)
    3. Transformation vers clean/ (CSV unique)
    4. Chargement dans Data Warehouse
    
    **Schedule** : Toutes les heures (@hourly)
    """
)

# ==============================================
# TÂCHES DU DAG
# ==============================================

# 1. Vérification de l'API
check_api = DummyOperator(
    task_id='check_api_availability',
    dag=dag,
)

# 2. Création des dossiers
create_dirs = DummyOperator(
    task_id='create_directories',
    dag=dag,
)

# 3. Extraction pour chaque ville (en parallèle)
with TaskGroup(group_id='extract_cities', dag=dag) as extract_group:
    extract_tasks = []
    for city in CITIES:
        task = PythonOperator(
            task_id=f'extract_{city.lower()}',
            python_callable=extract_air_quality_data,
            op_kwargs={
                'city_name': city,
                'api_key': API_KEY,
                'output_path': RAW_PATH,
            },
            dag=dag,
        )
        extract_tasks.append(task)

# 4. Transformation vers clean/
transform_task = PythonOperator(
    task_id='transform_to_clean',
    python_callable=transform_air_quality_data,
    op_kwargs={
        'raw_path': RAW_PATH,
        'clean_path': CLEAN_PATH,
    },
    dag=dag,
)

# 5. Validation du fichier clean/
validate_task = PythonOperator(
    task_id='validate_clean_file',
    python_callable=validate_clean_file,
    op_kwargs={
        'clean_path': CLEAN_PATH,
    },
    dag=dag,
)

# 6. Création des tables Data Warehouse
create_tables = PostgresOperator(
    task_id='create_dw_tables',
    postgres_conn_id='warehouse_postgres',
    sql="""
    CREATE TABLE IF NOT EXISTS dim_city (
        city_id SERIAL PRIMARY KEY,
        city_name VARCHAR(100) NOT NULL UNIQUE,
        country VARCHAR(50),
        latitude DECIMAL(10,6),
        longitude DECIMAL(10,6),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS dim_time (
        time_id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        hour INTEGER NOT NULL,
        day_of_week VARCHAR(20),
        day_of_week_num INTEGER,
        month INTEGER,
        year INTEGER,
        is_weekend BOOLEAN DEFAULT FALSE,
        UNIQUE(date, hour)
    );

    CREATE INDEX IF NOT EXISTS idx_dim_time_date ON dim_time(date);
    CREATE INDEX IF NOT EXISTS idx_dim_time_month ON dim_time(month, year);

    CREATE TABLE IF NOT EXISTS fact_air_quality (
        fact_id SERIAL PRIMARY KEY,
        city_id INTEGER REFERENCES dim_city(city_id),
        time_id INTEGER REFERENCES dim_time(time_id),
        aqi INTEGER,
        co DECIMAL(10,2),
        no DECIMAL(10,2),
        no2 DECIMAL(10,2),
        o3 DECIMAL(10,2),
        pm2_5 DECIMAL(10,2),
        pm10 DECIMAL(10,2),
        so2 DECIMAL(10,2),
        nh3 DECIMAL(10,2),
        measurement_timestamp TIMESTAMP,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(city_id, time_id)
    );

    CREATE INDEX IF NOT EXISTS idx_fact_city ON fact_air_quality(city_id);
    CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_air_quality(time_id);
    CREATE INDEX IF NOT EXISTS idx_fact_aqi ON fact_air_quality(aqi);
    """,
    dag=dag,
)

# 7. Chargement dans Data Warehouse
load_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    op_kwargs={
        'clean_path': CLEAN_PATH,
    },
    dag=dag,
)

# 8. Vérification de cohérence
check_coherence = PostgresOperator(
    task_id='check_coherence',
    postgres_conn_id='warehouse_postgres',
    sql="""
    DO $$
    DECLARE
        fact_count INTEGER;
        expected_count INTEGER;
    BEGIN
        SELECT COUNT(*) INTO fact_count FROM fact_air_quality;
        SELECT (SELECT COUNT(DISTINCT city_id) FROM dim_city) * 
               (SELECT COUNT(DISTINCT time_id) FROM dim_time) INTO expected_count;
        
        RAISE NOTICE 'Faits: %, Attendus: %', fact_count, expected_count;
    END $$;
    """,
    dag=dag,
)

# 9. Notification de succès
success_task = DummyOperator(
    task_id='success_notification',
    dag=dag,
)

# ==============================================
# DÉPENDANCES
# ==============================================

check_api >> create_dirs >> extract_group >> transform_task >> validate_task
validate_task >> create_tables >> load_task >> check_coherence >> success_task
