import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")

CITY = "Paris"
LAT = 48.8566
LON = 2.3522

RAW_FOLDER = "data/raw"


def fetch_air_quality():
    """
    Appel de l'API OpenWeather Air Pollution
    """

    url = (
        "http://api.openweathermap.org/data/2.5/air_pollution"
        f"?lat={LAT}&lon={LON}&appid={API_KEY}"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    return response.json()


def save_raw_data(data):
    """
    Sauvegarde de la réponse API en JSON
    """

    os.makedirs(RAW_FOLDER, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    filename = f"{RAW_FOLDER}/{CITY}_{timestamp}.json"

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    print(f"Fichier sauvegardé : {filename}")


def main():
    if not API_KEY:
        raise ValueError("OPENWEATHER_API_KEY manquante")

    data = fetch_air_quality()
    save_raw_data(data)


if __name__ == "__main__":
    main()