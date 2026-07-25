# Architecture — DataGreen / MangaRivotra

## Stack retenue

| Composant | Choix | Justification |
|---|---|---|
| **Orchestrateur** | Apache Airflow (LocalExecutor) | Déjà maîtrisé par l'équipe, planification cron native (`@hourly`), UI pour prouver l'historique des runs (exigé par le sujet), retries/alerting intégrés. |
| **Source de données** | OpenWeather Air Pollution API | Gratuite, couvre les 5 villes choisies, fournit AQI + 8 polluants (CO, NO, NO2, O3, PM2.5, PM10, SO2, NH3) à l'heure. |
| **Data Lake (raw/)** | Système de fichiers local monté en volume Docker (`data/raw/`) | Simplicité : pas de compte cloud/carte bancaire disponible pour le groupe. Un fichier JSON par ville et par appel, jamais réécrit. |
| **Data Lake (clean/)** | Fichier CSV unique reconstruit à chaque run (`data/clean/air_quality.csv`) | Respecte le contrat "un fichier unique, reconstruit depuis raw/" du sujet ; évite toute logique d'append/déduplication fragile. |
| **Data Warehouse** | PostgreSQL 14, modèle en **étoile** | Requêtable en SQL standard, un seul JOIN entre la table de faits et chaque dimension (pas de flocon nécessaire vu le faible nombre de dimensions). |
| **Conteneurisation** | Docker Compose | Portable, un seul `docker compose up -d` reproduit tout l'environnement pour les 5 membres et pour la soutenance. |
| **Stockage objet (MinIO)** | Optionnel / réserve | Prévu comme alternative S3-compatible à `raw/` si besoin de scaler, mais non requis pour livrer le contrat de données actuel. |

## Flux de données

OpenWeather API (air_pollution)
│ 1 appel / ville / heure (DAG @hourly)
▼
extract_air_quality_data() ──► data/raw/{ville}/{date}/{ville}_{horodatage}.json
│ (fichiers jamais modifiés après écriture)
▼
transform_air_quality_data() ──► data/clean/air_quality.csv
│ (relit TOUT raw/, dédoublonne ville+heure, trie, réécrit le même fichier)
▼
validate_clean_file() ──► vérifie schéma, colonnes, valeurs manquantes
▼
load_to_warehouse() ──► PostgreSQL : dim_city, dim_time, fact_air_quality (UPSERT)

## Modélisation dimensionnelle (étoile)

- **`dim_city`** : `city_id`, `city_name`, `country`, `latitude`, `longitude` — aucune mesure.
- **`dim_time`** : `time_id`, `date`, `hour`, `day_of_week`, `month`, `year`, `is_weekend` — aucune mesure.
- **`fact_air_quality`** : `fact_id`, `city_id` (FK), `time_id` (FK), `aqi`, `co`, `no`, `no2`, `o3`, `pm2_5`, `pm10`, `so2`, `nh3`, `measurement_timestamp` — uniquement des clés étrangères et des mesures, aucune colonne descriptive.

Cohérence attendue : `COUNT(fact_air_quality) ≈ nb_villes × nb_heures couvertes`. Les écarts (appels API échoués, période avant le premier backfill, etc.) sont documentés dans le README de stockage.

## Automatisation 24/7

- Tous les services Docker Compose tournent avec `restart: unless-stopped` : un redémarrage du serveur ou un crash de conteneur ne coupe pas le pipeline.
- Le DAG `air_quality_pipeline` est planifié `@hourly` avec `catchup=False` et continue de tourner après le rendu du projet, tant que le conteneur reste démarré.
- Le DAG `air_quality_backfill` est déclenché manuellement une fois pour peupler l'historique, puis n'a plus besoin d'intervention.

## Secrets

La clé API OpenWeather est injectée via une variable Airflow (`openweather_api_key`) ou la variable d'environnement `OPENWEATHER_API_KEY` chargée depuis `.env` (fichier non versionné, listé dans `.gitignore`). Elle n'apparaît jamais dans le code ni dans les logs.