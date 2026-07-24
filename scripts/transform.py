"""
Script de transformation des données brutes vers clean/
Génère un fichier CSV unique, dédoublonné et trié
"""

import pandas as pd
import json
import os
import glob
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Coordonnées des villes
CITY_COORDINATES = {
    "Paris": {"lat": 48.8566, "lon": 2.3522, "country": "FR"},
    "London": {"lat": 51.5074, "lon": -0.1278, "country": "UK"},
    "Berlin": {"lat": 52.5200, "lon": 13.4050, "country": "DE"},
    "Madrid": {"lat": 40.4168, "lon": -3.7038, "country": "ES"},
    "Rome": {"lat": 41.9028, "lon": 12.4964, "country": "IT"},
}

RAW_FOLDER = "data/raw"
CLEAN_FOLDER = "data/clean"


def transform_air_quality_data():
    """
    Transforme les données brutes en fichier clean/ unique
    """
    try:
        logger.info("🔄 Début de la transformation...")

        # Récupérer TOUS les fichiers JSON (incluant backfill_ et sous-dossiers)
        json_files = []
        
        # Fichiers à la racine de raw/
        json_files.extend(glob.glob(f"{RAW_FOLDER}/*.json"))
        
        # Fichiers dans les sous-dossiers (backfill/ ou autres)
        json_files.extend(glob.glob(f"{RAW_FOLDER}/**/*.json", recursive=True))
        
        # Alternative: chercher tous les fichiers JSON dans raw/ et sous-dossiers
        # json_files = list(Path(RAW_FOLDER).rglob("*.json"))

        if not json_files:
            logger.warning("Aucun fichier JSON trouvé dans raw/")
            return

        logger.info(f"📂 {len(json_files)} fichiers trouvés")

        all_records = []

        for file_path in json_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Gérer les deux formats: avec 'city' direct ou dans les métadonnées
                city = data.get("city")
                if not city and "data" in data and "city" in data["data"]:
                    city = data["data"].get("city")
                if not city:
                    city = data.get("city_name")
                
                if not city:
                    logger.warning(f"Ville non trouvée dans {file_path}, ignoré")
                    continue
                
                coords = CITY_COORDINATES.get(city, {})

                # Extraire les mesures
                entries = data.get("data", {}).get("list", [])
                if not entries and "list" in data:
                    entries = data.get("list", [])
                
                for entry in entries:
                    components = entry.get("components", {})
                    dt = entry.get("dt")

                    record = {
                        "city": city,
                        "country": coords.get("country"),
                        "latitude": coords.get("lat"),
                        "longitude": coords.get("lon"),
                        "timestamp": datetime.fromtimestamp(dt).isoformat() if dt else None,
                        "date": datetime.fromtimestamp(dt).strftime("%Y-%m-%d") if dt else None,
                        "hour": datetime.fromtimestamp(dt).hour if dt else None,
                        "day_of_week": datetime.fromtimestamp(dt).strftime("%A") if dt else None,
                        "month": datetime.fromtimestamp(dt).month if dt else None,
                        "year": datetime.fromtimestamp(dt).year if dt else None,
                        "is_weekend": datetime.fromtimestamp(dt).weekday() >= 5 if dt else None,
                        "aqi": entry.get("main", {}).get("aqi"),
                        "co": components.get("co"),
                        "no": components.get("no"),
                        "no2": components.get("no2"),
                        "o3": components.get("o3"),
                        "pm2_5": components.get("pm2_5"),
                        "pm10": components.get("pm10"),
                        "so2": components.get("so2"),
                        "nh3": components.get("nh3"),
                    }
                    all_records.append(record)
            except Exception as e:
                logger.error(f"❌ Erreur lors de la lecture de {file_path}: {e}")
                continue

        if not all_records:
            logger.warning("Aucune donnée extraite des fichiers")
            return

        # Création du DataFrame
        df = pd.DataFrame(all_records)

        # Nettoyage des données
        df = clean_dataframe(df)

        # Sauvegarde en CSV
        os.makedirs(CLEAN_FOLDER, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(CLEAN_FOLDER, f"air_quality_{timestamp}.csv")

        df.to_csv(output_file, index=False, encoding="utf-8")

        logger.info(f"✅ clean/ généré: {len(df)} lignes dans {output_file}")
        logger.info(f"📊 Colonnes: {list(df.columns)}")
        
        # Afficher les statistiques par ville
        if not df.empty and "city" in df.columns:
            logger.info(f"📊 Statistiques par ville:")
            city_stats = df["city"].value_counts()
            for city, count in city_stats.items():
                logger.info(f"  - {city}: {count} mesures")

        return output_file

    except Exception as e:
        logger.error(f"❌ Erreur de transformation: {e}")
        raise


def clean_dataframe(df):
    """
    Nettoie le DataFrame
    """
    # Supprimer les lignes sans ville ou timestamp
    df = df.dropna(subset=["city", "timestamp"])
    
    if df.empty:
        return df

    # Supprimer les doublons (même ville + même heure)
    df = df.drop_duplicates(subset=["city", "timestamp"], keep="last")

    # Trier par ville puis par timestamp
    df = df.sort_values(["city", "timestamp"])

    # Réinitialiser l'index
    df = df.reset_index(drop=True)

    return df


def main():
    """Point d'entrée principal"""
    logger.info("🚀 Lancement de la transformation...")
    result = transform_air_quality_data()
    if result:
        logger.info(f"✅ Transformation terminée avec succès: {result}")
    else:
        logger.error("❌ La transformation a échoué")


if __name__ == "__main__":
    main()