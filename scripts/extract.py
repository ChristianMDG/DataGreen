"""
Script d'extraction des données de qualité de l'air (OpenWeather API)

Contient deux familles de fonctions :
- extract_air_quality_data() : extraction "temps réel" pour UNE ville,
  utilisée par le DAG horaire air_quality_pipeline.
- fetch_historical_data() / main() : extraction historique (backfill),
  utilisées par air_quality_backfill.py / extract_backfill.py.
"""

import os
import json
import requests
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.models import Variable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Configuration - Récupérer la clé API depuis Airflow
API_KEY = Variable.get("openweather_api_key", default_var=None)

if not API_KEY:
    logger.warning("⚠️ OPENWEATHER_API_KEY non définie dans Airflow")
    logger.info("👉 Définissez-la avec: airflow variables set openweather_api_key 'votre_cle'")

# Villes
CITIES = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "London": {"lat": 51.5074, "lon": -0.1278},
    "Berlin": {"lat": 52.5200, "lon": 13.4050},
    "Madrid": {"lat": 40.4168, "lon": -3.7038},
    "Rome": {"lat": 41.9028, "lon": 12.4964}
}

RAW_FOLDER = "data/raw"

AIR_QUALITY_URL = "http://api.openweathermap.org/data/2.5/air_pollution"


def extract_air_quality_data(city_name, api_key, output_path, retries=3):
    """
    Extraction "temps réel" de la qualité de l'air pour UNE ville.

    Appelée toutes les heures par le DAG air_quality_pipeline (une tâche
    par ville, en parallèle via un TaskGroup). Écrit un fichier JSON brut
    et NE LE MODIFIE JAMAIS ENSUITE (zone raw/ intouchable) :
    raw/{city}/{YYYY-MM-DD}/{city}_{YYYYMMDDTHHMMSS}.json

    Args:
        city_name: nom de la ville (doit être une clé de CITIES)
        api_key: clé OpenWeather (ne jamais logger sa valeur)
        output_path: dossier racine raw/ (ex: /opt/airflow/data/raw)

    Returns:
        dict avec au minimum {"city": ..., "status": "success"|"error"}
    """
    coords = CITIES.get(city_name)
    if coords is None:
        logger.error(f"❌ Ville inconnue: {city_name}")
        return {"city": city_name, "status": "error", "reason": "unknown_city"}

    if not api_key:
        logger.error(f"❌ Clé API manquante, extraction impossible pour {city_name}")
        return {"city": city_name, "status": "error", "reason": "missing_api_key"}

    params = {"lat": coords["lat"], "lon": coords["lon"], "appid": api_key}

    data = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(AIR_QUALITY_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            break
        except requests.exceptions.RequestException as e:
            logger.warning(f"Tentative {attempt}/{retries} échouée pour {city_name}: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt * 5)
            else:
                logger.error(f"❌ Échec définitif de l'extraction pour {city_name}: {e}")
                return {"city": city_name, "status": "error", "reason": str(e)}

    now = datetime.now()
    city_folder = os.path.join(output_path, city_name.lower(), now.strftime("%Y-%m-%d"))
    os.makedirs(city_folder, exist_ok=True)

    filename = os.path.join(
        city_folder,
        f"{city_name.lower()}_{now.strftime('%Y%m%dT%H%M%S')}.json"
    )

    payload = {
        "city": city_name,
        "country": None,
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "data": data,
        "timestamp": now.isoformat(),
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    nb_mesures = len(data.get("list", [])) if data else 0
    logger.info(f"✅ {city_name}: {nb_mesures} mesure(s) sauvegardée(s) -> {filename}")

    return {"city": city_name, "status": "success", "file": filename}


def fetch_historical_data(city, lat, lon, start_date, end_date, retries=3):
    """Extrait les données historiques pour une ville"""
    if not API_KEY:
        logger.warning(f"⚠️ Clé API manquante, backfill simulé pour {city}")
        return {
            "city": city,
            "data": {"list": []},
            "timestamp": datetime.now().isoformat()
        }
    
    url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
    
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())
    
    params = {
        "lat": lat,
        "lon": lon,
        "start": start_ts,
        "end": end_ts,
        "appid": API_KEY
    }
    
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"✅ {city}: {len(data.get('list', []))} mesures extraites")
            return {
                "city": city,
                "data": data,
                "timestamp": datetime.now().isoformat()
            }
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Tentative {attempt}/{retries} échouée pour {city}: {e}")
            if attempt < retries:
                wait_time = 2 ** attempt * 10
                logger.info(f"Nouvelle tentative dans {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Échec définitif pour {city}")
                return None


def save_backfill_data(city, data):
    """Sauvegarde les données historiques"""
    if not data:
        return
    
    os.makedirs(RAW_FOLDER, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{RAW_FOLDER}/backfill_{city}_{timestamp}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    
    logger.info(f"💾 Fichier sauvegardé: {filename}")
    return filename


def split_date_range(start_date, end_date, days=30):
    """Divise la période en intervalles de N jours"""
    intervals = []
    current = start_date
    
    while current < end_date:
        interval_end = min(current + timedelta(days=days), end_date)
        intervals.append((current, interval_end))
        current = interval_end
    
    return intervals


def main():
    """Point d'entrée principal"""
    if not API_KEY:
        logger.error("❌ Backfill impossible: clé API manquante")
        logger.info("👉 Définissez la variable: airflow variables set openweather_api_key 'votre_cle'")
        return
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    logger.info(f"📅 Période: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"🏙️ Villes: {', '.join(CITIES.keys())}")
    
    intervals = split_date_range(start_date, end_date, days=30)
    logger.info(f"📊 {len(intervals)} intervalles de 30 jours")
    
    total_calls = len(CITIES) * len(intervals)
    logger.info(f"📊 Total API calls: {total_calls}")
    
    progress = {"completed": 0, "total": total_calls}
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        
        for interval_start, interval_end in intervals:
            for city, coords in CITIES.items():
                future = executor.submit(
                    fetch_historical_data,
                    city,
                    coords["lat"],
                    coords["lon"],
                    interval_start,
                    interval_end
                )
                futures.append((future, city))
        
        for future, city in futures:
            result = future.result()
            progress["completed"] += 1
            
            if result:
                save_backfill_data(city, result)
            
            if progress["completed"] % 5 == 0:
                pct = (progress["completed"] / progress["total"]) * 100
                logger.info(f"📊 Progression: {progress['completed']}/{progress['total']} ({pct:.1f}%)")
    
    logger.info("✅ Backfill terminé !")


if __name__ == "__main__":
    main()