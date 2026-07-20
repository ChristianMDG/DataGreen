import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")

CITIES = {
    "Paris": (48.8566, 2.3522),
    "London": (51.5074, -0.1278),
    "Berlin": (52.5200, 13.4050),
    "Madrid": (40.4168, -3.7038),
    "Rome": (41.9028, 12.4964)
}

RAW_FOLDER = "data/raw"


def fetch_air_quality(city, lat, lon):

    url = (
        "http://api.openweathermap.org/data/2.5/air_pollution"
        f"?lat={lat}&lon={lon}&appid={API_KEY}"
    )

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        logger.info(f"Extraction réussie : {city}")

        return {
            "city": city,
            "data": response.json(),
            "timestamp": datetime.now().isoformat()
        }

    except requests.exceptions.Timeout:
        logger.error(f"Timeout API pour {city}")
        return None

    except requests.exceptions.HTTPError as e:
        logger.error(f"Erreur HTTP pour {city}: {e}")
        return None

    except Exception as e:
        logger.error(f"Erreur inconnue pour {city}: {e}")
        return None


def save_raw_data(city, data):
    """
    Sauvegarde de la réponse API en JSON
    """

    os.makedirs(RAW_FOLDER, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    filename = f"{RAW_FOLDER}/{city}_{timestamp}.json"

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    print(f"Fichier sauvegardé : {filename}")

def main():
    if not API_KEY:
        raise ValueError("OPENWEATHER_API_KEY manquante")

    with ThreadPoolExecutor(max_workers=5) as executor:

        futures = [
            executor.submit(
                fetch_air_quality,
                city,
                lat,
                lon
            )
            for city, (lat, lon) in CITIES.items()
        ]

        for future in as_completed(futures):
            result = future.result()

            save_raw_data(
                result["city"],
                result
            )


if __name__ == "__main__":
    main()