# Documentation du stockage — DataGreen

## Villes couvertes

| Ville | Pays | Latitude | Longitude |
|---|---|---|---|
| Paris | FR | 48.8566 | 2.3522 |
| London | UK | 51.5074 | -0.1278 |
| Berlin | DE | 52.5200 | 13.4050 |
| Madrid | ES | 40.4168 | -3.7038 |
| Rome | IT | 41.9028 | 12.4964 |

## `raw/` — zone intouchable

- Chemin : `data/raw/{ville_minuscule}/{YYYY-MM-DD}/{ville}_{YYYYMMDDTHHMMSS}.json`
- Un fichier par ville et par appel API (extraction horaire + backfill).
- Contenu brut de la réponse OpenWeather `air_pollution` (extraction horaire) ou `air_pollution/history` (backfill), jamais modifié après écriture.

## `clean/` — contrat de données

- **Un seul fichier** : `data/clean/air_quality.csv`, entièrement reconstruit depuis `raw/` à chaque exécution du DAG (pas d'append).
- Une ligne = une ville + une heure. Triée par ville puis par horodatage. Dédoublonnée sur `(city, timestamp)`.

| Colonne | Type | Unité / format | Description |
|---|---|---|---|
| `city` | texte | — | Nom de la ville |
| `country` | texte | code ISO 2 lettres | Pays de la ville |
| `latitude` | décimal | degrés | Latitude de la ville |
| `longitude` | décimal | degrés | Longitude de la ville |
| `timestamp` | texte | ISO 8601 | Horodatage complet de la mesure |
| `date` | texte | YYYY-MM-DD | Date de la mesure |
| `hour` | entier | 0-23 | Heure de la mesure |
| `day_of_week` | texte | nom du jour (EN) | Jour de la semaine |
| `month` | entier | 1-12 | Mois |
| `year` | entier | AAAA | Année |
| `is_weekend` | booléen | true/false | Samedi ou dimanche |
| `aqi` | entier | 1 (Good) à 5 (Very Poor) | Indice de qualité de l'air OpenWeather |
| `co` | décimal | µg/m³ | Monoxyde de carbone |
| `no` | décimal | µg/m³ | Monoxyde d'azote |
| `no2` | décimal | µg/m³ | Dioxyde d'azote |
| `o3` | décimal | µg/m³ | Ozone |
| `pm2_5` | décimal | µg/m³ | Particules fines ≤ 2.5 µm |
| `pm10` | décimal | µg/m³ | Particules fines ≤ 10 µm |
| `so2` | décimal | µg/m³ | Dioxyde de soufre |
| `nh3` | décimal | µg/m³ | Ammoniac |

## Warehouse (PostgreSQL, modèle en étoile)

Voir `sql/create_dw.sql` pour le DDL complet.

- `dim_city(city_id, city_name, country, latitude, longitude)`
- `dim_time(time_id, date, hour, day_of_week, day_of_week_num, month, year, is_weekend)`
- `fact_air_quality(fact_id, city_id FK, time_id FK, aqi, co, no, no2, o3, pm2_5, pm10, so2, nh3, measurement_timestamp)`

Cohérence : `COUNT(fact_air_quality) ≈ nb_villes (5) × nb_heures couvertes`. Les écarts viennent d'éventuels échecs d'appel API (retries épuisés) ou de la période précédant le premier backfill — à mettre à jour avec les chiffres réels une fois le backfill exécuté.

## Période couverte

_À compléter après exécution du backfill_ (`air_quality_backfill` DAG) : date de début, date de fin, nombre total de mesures, trous connus par ville.

## Connexion à la base (développement local)

```
Host: localhost
Port: 5433
Database: air_quality_db
User: warehouse
Password: warehouse   # via .env en local, jamais en dur en production
```