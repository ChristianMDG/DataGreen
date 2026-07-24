"""
DAG Airflow - Backfill des données historiques
Extraction des 12 derniers mois depuis OpenWeather API

Objectif: Remplir l'historique de 12 mois (minimum 4 mois exigé)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import logging
import sys

sys.path.append('/opt/airflow/scripts')

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'air_quality_backfill',
    default_args=DEFAULT_ARGS,
    description='Backfill 12 mois de données de qualité de l\'air',
    schedule_interval=None,  # Déclenchement manuel uniquement
    tags=['air_quality', 'backfill', 'historical', 'iai'],
    max_active_runs=1,
    doc_md="""
    ### 📥 Backfill des données historiques
    
    **Objectif** : Remplir l'historique de 12 mois de données
    
    **Période** : 12 derniers mois (minimum 4 mois exigé par le sujet)
    **Villes** : Paris, London, Berlin, Madrid, Rome
    **API** : OpenWeather History
    
    **Déclenchement** : Manuel uniquement
    **Durée estimée** : 1-2 heures (65 appels API)
    """
)

with dag:
    start = DummyOperator(task_id='start', doc_md="🚀 Début du backfill")
    
    # Importer la fonction de backfill
    from extract_backfill import main as backfill_main
    
    backfill_task = PythonOperator(
        task_id='extract_historical_data',
        python_callable=backfill_main,
        doc_md="📥 Extraction des données historiques pour 5 villes (12 mois)"
    )
    
    # Après le backfill, déclencher le pipeline normal
    trigger_pipeline = DummyOperator(
        task_id='trigger_pipeline',
        doc_md="🔄 Déclenchement du pipeline ETL après le backfill"
    )
    
    end = DummyOperator(task_id='end', doc_md="🏁 Fin du backfill")
    
    start >> backfill_task >> trigger_pipeline >> end
