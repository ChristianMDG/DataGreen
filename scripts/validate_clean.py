"""
Script de validation du fichier clean/
Vérifie la structure, les colonnes et la qualité des données
"""

import pandas as pd
import os
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

CLEAN_FOLDER = "data/clean"


def validate_clean_file(clean_path=None):
    """
    Valide le fichier clean/
    """
    try:
        if clean_path is None:
            clean_path = CLEAN_FOLDER

        # Trouver le dernier fichier CSV
        clean_files = sorted(
            [f for f in os.listdir(clean_path) if f.endswith(".csv")],
            reverse=True
        )

        if not clean_files:
            logger.warning("⚠️ Aucun fichier clean/ trouvé")
            return False

        latest_file = os.path.join(clean_path, clean_files[0])
        df = pd.read_csv(latest_file)

        logger.info(f"📂 Validation de {latest_file}")
        logger.info(f"   Lignes: {len(df)}")
        logger.info(f"   Colonnes: {list(df.columns)}")

        # 1. Vérifier les colonnes obligatoires
        required_cols = ["city", "timestamp", "aqi", "pm2_5", "pm10"]
        missing = [col for col in required_cols if col not in df.columns]

        if missing:
            logger.error(f"❌ Colonnes manquantes: {missing}")
            return False

        logger.info("✅ Colonnes obligatoires présentes")

        # 2. Vérifier les valeurs manquantes
        missing_values = df[required_cols].isnull().sum()
        if missing_values.any():
            logger.warning(f"⚠️ Valeurs manquantes: {missing_values.to_dict()}")

        # 3. Vérifier les doublons
        duplicates = df.duplicated(subset=["city", "timestamp"]).sum()
        if duplicates > 0:
            logger.warning(f"⚠️ {duplicates} doublons trouvés")
        else:
            logger.info("✅ Pas de doublons")

        # 4. Vérifier les villes
        expected_cities = ["Paris", "London", "Berlin", "Madrid", "Rome"]
        present_cities = df["city"].unique().tolist()
        missing_cities = [c for c in expected_cities if c not in present_cities]

        if missing_cities:
            logger.warning(f"⚠️ Villes manquantes: {missing_cities}")
        else:
            logger.info(f"✅ Toutes les villes présentes: {present_cities}")

        # 5. Vérifier les plages AQI
        aqi_valid = df["aqi"].between(1, 5).all()
        if aqi_valid:
            logger.info(f"✅ AQI valides (1-5): min={df['aqi'].min()}, max={df['aqi'].max()}")
        else:
            logger.warning(f"⚠️ AQI hors plage: min={df['aqi'].min()}, max={df['aqi'].max()}")

        # 6. Vérifier les dates
        date_range = (pd.to_datetime(df["date"]).max() - pd.to_datetime(df["date"]).min()).days
        logger.info(f"📅 Période: {df['date'].min()} à {df['date'].max()} ({date_range} jours)")

        logger.info("✅ Validation clean/ réussie")
        return True

    except Exception as e:
        logger.error(f"❌ Erreur de validation: {e}")
        return False


def main():
    """Point d'entrée principal"""
    logger.info("🚀 Lancement de la validation...")
    result = validate_clean_file()
    if result:
        print("\n✅ Validation clean/ : SUCCÈS")
    else:
        print("\n❌ Validation clean/ : ÉCHEC")


if __name__ == "__main__":
    main()