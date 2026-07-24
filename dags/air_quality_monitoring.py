"""
DAG Airflow - Monitoring de la qualité des données
Vérification quotidienne de la qualité et des alertes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'air_quality_monitoring',
    default_args=DEFAULT_ARGS,
    description='Monitoring quotidien de la qualite des donnees',
    schedule_interval='@daily',
    tags=['air_quality', 'monitoring', 'iai'],
    max_active_runs=1,
    doc_md="""
    ### Monitoring de la qualite des donnees
    
    **Objectif** : Verifier quotidiennement la qualite des donnees
    
    **Verifications** :
    1. Donnees manquantes par ville
    2. Alertes AQI > 150
    3. Coherence des donnees
    4. Generation de rapport
    
    **Schedule** : Tous les jours a 08:00
    """
)

with dag:
    start = DummyOperator(task_id='start', doc_md="Debut du monitoring")
    
    check_missing = PostgresOperator(
        task_id='check_missing_data',
        postgres_conn_id='warehouse_postgres',
        sql="""
        SELECT 
            c.city_name,
            COUNT(*) as nb_mesures,
            MAX(t.date) as last_date
        FROM fact_air_quality f
        JOIN dim_city c ON f.city_id = c.city_id
        JOIN dim_time t ON f.time_id = t.time_id
        GROUP BY c.city_name
        ORDER BY nb_mesures;
        """,
        doc_md="Verification des donnees manquantes par ville"
    )
    
    check_alerts = PostgresOperator(
        task_id='check_aqi_alerts',
        postgres_conn_id='warehouse_postgres',
        sql="""
        SELECT 
            c.city_name,
            t.date,
            t.hour,
            f.aqi,
            f.pm2_5,
            f.pm10
        FROM fact_air_quality f
        JOIN dim_city c ON f.city_id = c.city_id
        JOIN dim_time t ON f.time_id = t.time_id
        WHERE f.aqi > 150
        AND t.date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY f.aqi DESC;
        """,
        doc_md="Verification des alertes AQI > 150"
    )
    
    generate_report = DummyOperator(
        task_id='generate_quality_report',
        doc_md="Generation du rapport de qualite"
    )
    
    send_notification = DummyOperator(
        task_id='send_notification',
        doc_md="Envoi de la notification"
    )
    
    end = DummyOperator(task_id='end', doc_md="Fin du monitoring")
    
    start >> check_missing >> check_alerts >> generate_report >> send_notification >> end
