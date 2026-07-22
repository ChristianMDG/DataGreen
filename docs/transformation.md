## Regles de Nettoyage

- Conversion des timestamps UNIX en datetime
- Gestion des valeurs manquantes (None par defaut)
- Extraction des polluants depuis le champ components
- Suppression des colonnes non utilisees

## Features Crees

| Feature | Description | Source |
|---------|-------------|--------|
| date | Date de la mesure | dt |
| hour | Heure (0-23) | dt |
| day | Jour de semaine | dt |
| month | Mois (1-12) | dt |
| year | Annee | dt |
| season | Saison (Winter, Spring, Summer, Autumn) | month |
| weekend | True si Samedi/Dimanche | dt |
| aqi_cat | Categorie AQI (Good, Fair, Moderate, Poor, Very Poor) | aqi |
| pm_ratio | Ratio PM2.5/PM10 | pm2_5 / pm10 |
| quality | Indicateur qualite (Good, Bad, Very Bad) | aqi |

## Structure des Donnees

**Version Basique (11 colonnes)**
city, date, hour, aqi, pm2_5, pm10, co, no, no2, o3, so2

**Version Complete (19 colonnes)**
city, date, hour, day, month, year, season, weekend, aqi, pm2_5, pm10, co, no, no2, o3, so2, pm_ratio, aqi_cat, quality

**Version Finale (19 colonnes + partitionnee)**
city, date, hour, day, month, year, season, weekend, aqi, pm2_5, pm10, co, no, no2, o3, so2, pm_ratio, aqi_cat, quality
(Partitionnee par date dans dossiers YYYY-MM-DD/)

## Pre-requis

```bash
pip3 install pandas numpy pyarrow

## Guide d'Execution

```bash
cd ~/DataGreen/scripts
python3 transform.py
python3 quality_checks.py

cd ~/DataGreen/tests
python3 test_transform.py
python3 test_quality.py
