"""
Script d'extraction des données historiques (Backfill)
Extraction des 12 derniers mois depuis OpenWeather API
"""

import os
import json
import requests
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Configuration
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY manquante")

# Villes
CITIES = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "London": {"lat": 51.5074, "lon": -0.1278},
    "Berlin": {"lat": 52.5200, "lon": 13.4050},
    "Madrid": {"lat": 40.4168, "lon": -3.7038},
    "Rome": {"lat": 41.9028, "lon": 12.4964}
}

RAW_FOLDER = "data/raw"


def fetch_historical_data(city, lat, lon, start_date, end_date, retries=3):
    """Extrait les données historiques pour une ville"""
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
