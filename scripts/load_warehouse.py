import pandas as pd
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CLEAN_FOLDER = "data/clean"
DB_CONFIG = {
    "host": "postgres_warehouse",
    "port": 5432,
    "database": "air_quality_db",
    "user": "warehouse",
    "password": "warehouse"
}

def get_engine():
    return create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

def load_to_warehouse(clean_path=None):
    try:
        if clean_path is None:
            clean_path = CLEAN_FOLDER

        clean_files = sorted([f for f in os.listdir(clean_path) if f.endswith(".csv")], reverse=True)
        if not clean_files:
            logger.warning("Aucun fichier clean/ trouvé")
            return

        latest_file = os.path.join(clean_path, clean_files[0])
        df = pd.read_csv(latest_file)
        logger.info(f"📂 Chargement de {len(df)} lignes depuis {latest_file}")

        engine = get_engine()

        with engine.connect() as conn:
            # 1. Charger dim_city (force)
            logger.info("🏙️ Chargement dim_city...")
            cities = df[["city", "country", "latitude", "longitude"]].drop_duplicates()
            for _, row in cities.iterrows():
                query = text("""
                    INSERT INTO dim_city (city_name, country, latitude, longitude)
                    VALUES (:city, :country, :lat, :lon)
                    ON CONFLICT (city_name) DO UPDATE SET
                        country = EXCLUDED.country,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude
                """)
                conn.execute(query, {
                    "city": str(row["city"]),
                    "country": str(row["country"]) if pd.notna(row["country"]) else None,
                    "lat": float(row["latitude"]) if pd.notna(row["latitude"]) else None,
                    "lon": float(row["longitude"]) if pd.notna(row["longitude"]) else None
                })
            logger.info(f"✅ {len(cities)} villes chargées")

            # 2. Charger dim_time (force)
            logger.info("⏰ Chargement dim_time...")
            times = df[["date", "hour", "day_of_week", "month", "year", "is_weekend"]].drop_duplicates()
            times = times.dropna(subset=["date", "hour"])
            
            for _, row in times.iterrows():
                date_obj = datetime.strptime(row["date"], "%Y-%m-%d")
                day_of_week_num = date_obj.weekday() + 1
                
                query = text("""
                    INSERT INTO dim_time (date, hour, day_of_week, day_of_week_num, month, year, is_weekend)
                    VALUES (:date, :hour, :day_of_week, :day_of_week_num, :month, :year, :is_weekend)
                    ON CONFLICT (date, hour) DO UPDATE SET
                        day_of_week = EXCLUDED.day_of_week,
                        day_of_week_num = EXCLUDED.day_of_week_num,
                        month = EXCLUDED.month,
                        year = EXCLUDED.year,
                        is_weekend = EXCLUDED.is_weekend
                """)
                conn.execute(query, {
                    "date": row["date"],
                    "hour": int(row["hour"]),
                    "day_of_week": str(row["day_of_week"]),
                    "day_of_week_num": day_of_week_num,
                    "month": int(row["month"]),
                    "year": int(row["year"]),
                    "is_weekend": row["is_weekend"]
                })
            logger.info(f"✅ {len(times)} temps chargés")

            # 3. Récupérer les mappings (avec conversion explicite)
            city_map = {}
            result = conn.execute(text("SELECT city_id, city_name FROM dim_city"))
            for row in result:
                city_map[str(row[1]).strip()] = row[0]
            logger.info(f"✅ Mapping villes: {city_map}")

            time_map = {}
            result = conn.execute(text("SELECT time_id, date, hour FROM dim_time"))
            for row in result:
                time_map[(str(row[1]), int(row[2]))] = row[0]
            logger.info(f"✅ Mapping temps: {len(time_map)} entrées")

            # 4. Vérifier un échantillon
            logger.info("🔍 Vérification d'un échantillon...")
            sample = df.iloc[0]
            sample_city = str(sample["city"]).strip()
            sample_date = str(sample["date"]).strip()
            sample_hour = int(sample["hour"])
            
            logger.info(f"   City: '{sample_city}' → ID: {city_map.get(sample_city)}")
            logger.info(f"   Time: '{sample_date}' {sample_hour}h → ID: {time_map.get((sample_date, sample_hour))}")

            # 5. Chargement fact_air_quality
            logger.info("📊 Chargement fact_air_quality...")
            
            inserted = 0
            total_rows = len(df)
            
            # Préparer les données en vérifiant les mappings
            records_to_insert = []
            missing_city = 0
            missing_time = 0
            
            for idx, row in df.iterrows():
                city = str(row["city"]).strip()
                city_id = city_map.get(city)
                if city_id is None:
                    missing_city += 1
                    continue
                
                date = str(row["date"]).strip()
                hour = int(row["hour"])
                time_id = time_map.get((date, hour))
                if time_id is None:
                    missing_time += 1
                    continue
                
                records_to_insert.append({
                    "city_id": city_id,
                    "time_id": time_id,
                    "aqi": float(row["aqi"]) if pd.notna(row["aqi"]) else None,
                    "co": float(row["co"]) if pd.notna(row["co"]) else None,
                    "no": float(row["no"]) if pd.notna(row["no"]) else None,
                    "no2": float(row["no2"]) if pd.notna(row["no2"]) else None,
                    "o3": float(row["o3"]) if pd.notna(row["o3"]) else None,
                    "pm2_5": float(row["pm2_5"]) if pd.notna(row["pm2_5"]) else None,
                    "pm10": float(row["pm10"]) if pd.notna(row["pm10"]) else None,
                    "so2": float(row["so2"]) if pd.notna(row["so2"]) else None,
                    "nh3": float(row["nh3"]) if pd.notna(row["nh3"]) else None,
                    "timestamp": row["timestamp"]
                })
            
            logger.info(f"✅ {len(records_to_insert)} enregistrements à insérer")
            logger.info(f"   ❌ Villes manquantes: {missing_city}")
            logger.info(f"   ❌ Temps manquants: {missing_time}")

            if records_to_insert:
                # Démarrer une transaction
                trans = conn.begin()
                try:
                    batch_size = 1000
                    for i in range(0, len(records_to_insert), batch_size):
                        batch = records_to_insert[i:i+batch_size]
                        for record in batch:
                            query = text("""
                                INSERT INTO fact_air_quality (
                                    city_id, time_id, aqi, co, no, no2, o3,
                                    pm2_5, pm10, so2, nh3, measurement_timestamp
                                ) VALUES (
                                    :city_id, :time_id, :aqi, :co, :no, :no2, :o3,
                                    :pm2_5, :pm10, :so2, :nh3, :timestamp
                                )
                                ON CONFLICT (city_id, time_id) DO NOTHING
                            """)
                            result = conn.execute(query, record)
                            if result.rowcount > 0:
                                inserted += 1
                        
                        logger.info(f"   Batch {i//batch_size + 1}/{len(records_to_insert)//batch_size + 1}: {inserted} insérées")
                    
                    trans.commit()
                    logger.info(f"✅ {inserted} lignes insérées dans fact_air_quality")

                except Exception as e:
                    trans.rollback()
                    logger.error(f"❌ Erreur d'insertion: {e}")
                    raise

            # 5. Vérification
            result = conn.execute(text("SELECT COUNT(*) FROM fact_air_quality"))
            total = result.fetchone()[0]
            logger.info(f"📊 Total dans fact_air_quality: {total} lignes")

            # 6. Statistiques par ville
            if total > 0:
                result = conn.execute(text("""
                    SELECT c.city_name, COUNT(*) 
                    FROM fact_air_quality f 
                    JOIN dim_city c ON f.city_id = c.city_id 
                    GROUP BY c.city_name 
                    ORDER BY c.city_name
                """))
                logger.info("📊 Statistiques par ville:")
                for row in result:
                    logger.info(f"   - {row[0]}: {row[1]} mesures")

            return {"status": "success", "inserted": inserted, "total": total}

    except Exception as e:
        logger.error(f"❌ Erreur de chargement: {e}")
        raise

def main():
    logger.info("🚀 Lancement du chargement...")
    result = load_to_warehouse()
    if result:
        print(f"\n✅ Chargement terminé: {result['inserted']} lignes insérées")
        print(f"   Total: {result['total']} lignes dans fact_air_quality")

if __name__ == "__main__":
    main()
