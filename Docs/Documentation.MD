# 📚 DOCUMENTATION COMPLÈTE DU PROJET - ÉQUIPE AIR QUALITY

---

## 📋 TABLE DES MATIÈRES

1. [Présentation du Projet](#1-présentation-du-projet)
2. [Architecture Technique](#2-architecture-technique)
3. [Guide d'Installation](#3-guide-dinstallation)
4. [Structure du Projet](#4-structure-du-projet)
5. [Rôles et Responsabilités](#5-rôles-et-responsabilités)
6. [Guide de Développement](#6-guide-de-développement)
7. [Guide d'Utilisation](#7-guide-dutilisation)
8. [Tests et Qualité](#8-tests-et-qualité)
9. [Déploiement](#9-déploiement)
10. [FAQ et Dépannage](#10-faq-et-dépannage)
11. [Annexes](#11-annexes)

---

## 1. PRÉSENTATION DU PROJET

### 1.1 Objectif du Projet

Créer un pipeline de données automatisé pour collecter, transformer et analyser les données de qualité de l'air de 5 grandes villes européennes (Paris, London, Berlin, Madrid, Rome) à partir de l'API OpenWeather.

### 1.2 Livrables Attendus

- ✅ Pipeline ETL complet automatisé
- ✅ Data Warehouse avec modèle en étoile
- ✅ Dashboard de visualisation
- ✅ Documentation technique
- ✅ Vidéo de démonstration (3 minutes)
- ✅ Code versionné sur GitHub

### 1.3 Équipe Projet

| Membre | Rôle | Email | GitHub |
|--------|------|-------|--------|
| **Chrstian** | Lead Architecte | lead@airquality.com | @lead-dev |
| **Nomena** | Data Engineer - Extraction | nomena@airquality.com | @nomena-dev |
| **Miharintsoa** | Data Engineer - Transformation | miharintsoa@airquality.com | @miharintsoa-dev |
| **Gael** | Data Analyst / BI | lucas@airquality.com | @lucas-dev |

### 1.4 Technologies Utilisées

| Catégorie | Technologie | Version | Utilisation |
|-----------|-------------|---------|-------------|
| Orchestration | Apache Airflow | 2.7.0 | Pipeline ETL |
| Base de données | PostgreSQL | 14 | Métadonnées & Data Warehouse |
| Stockage | MinIO / AWS S3 | Latest | Data Lake |
| Transformation | Python + Pandas | 3.9 | Nettoyage des données |
| Visualisation | Apache Superset | 2.1.0 | Dashboard |
| Conteneurisation | Docker + Docker Compose | Latest | Environnement |
| Versionnement | Git + GitHub | - | Code |

---

## 2. ARCHITECTURE TECHNIQUE

### 2.1 Architecture Globale

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ARCHITECTURE GLOBALE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    ORCHESTRATION LAYER                               │  │
│  │                    Apache Airflow                                    │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐   │  │
│  │  │  Webserver │  │ Scheduler  │  │  Worker    │  │   Redis    │   │  │
│  │  │   :8080    │  │            │  │  (Celery)  │  │  (Broker)  │   │  │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                      DATA PIPELINE LAYER                            │  │
│  │                                                                      │  │
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐         │  │
│  │  │  Extraction  │───▶│  Transform   │───▶│    Load      │         │  │
│  │  │    (Nomena)  │    │ (Miharintsoa)│    │   (Lucas)    │         │  │
│  │  └──────────────┘    └──────────────┘    └──────────────┘         │  │
│  │        │                    │                    │                  │  │
│  │        ▼                    ▼                    ▼                  │  │
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐         │  │
│  │  │  Raw Data    │    │  Clean Data  │    │  Data        │         │  │
│  │  │  (JSON)      │    │  (Parquet)   │    │  Warehouse   │         │  │
│  │  │  S3/MinIO    │    │  S3/MinIO    │    │  PostgreSQL  │         │  │
│  │  └──────────────┘    └──────────────┘    └──────────────┘         │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    VISUALIZATION LAYER                               │  │
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐         │  │
│  │  │   Superset   │    │    Grafana   │    │   Looker     │         │  │
│  │  │  Dashboard   │    │  Monitoring  │    │   Studio     │         │  │
│  │  └──────────────┘    └──────────────┘    └──────────────┘         │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Flux de Données

```
1. API OpenWeather (Toutes les heures)
   ↓
2. Extraction (Lambda/PythonOperator)
   ↓
3. Stockage Brut (S3/MinIO - JSON)
   ↓
4. Transformation (Pandas)
   ↓
5. Stockage Nettoyé (S3/MinIO - Parquet)
   ↓
6. Chargement (UPSERT)
   ↓
7. Data Warehouse (PostgreSQL - Star Schema)
   ↓
8. Dashboard (Superset)
```

### 2.3 Modèle de Données (Star Schema)

```
                         ┌──────────────────┐
                         │   dim_city       │
                         ├──────────────────┤
                         │ city_id (PK)     │
                         │ city_name        │
                         │ country          │
                         │ latitude         │
                         │ longitude        │
                         └────────┬─────────┘
                                  │
                                  │
    ┌──────────────────┐         │          ┌──────────────────┐
    │   dim_time       │         │          │   fact_air_      │
    ├──────────────────┤         │          │   quality        │
    │ time_id (PK)     │         │          ├──────────────────┤
    │ date             │─────────┼─────────▶│ fact_id (PK)     │
    │ hour             │         │          │ city_id (FK)     │
    │ day_of_week      │         │          │ time_id (FK)     │
    │ month            │         │          │ aqi              │
    │ year             │         │          │ co               │
    │ is_weekend       │         │          │ no               │
    └──────────────────┘         │          │ no2              │
                                 │          │ o3               │
                                 │          │ pm2_5            │
                                 │          │ pm10             │
                                 │          │ so2              │
                                 │          │ nh3              │
                                 │          │ timestamp        │
                                 └──────────┴──────────────────┘
                                  │
                                  │
                         ┌────────┴─────────┐
                         │   dim_weather    │
                         ├──────────────────┤
                         │ weather_id (PK)  │
                         │ temperature      │
                         │ humidity         │
                         │ pressure         │
                         │ wind_speed       │
                         │ condition        │
                         └──────────────────┘
```

---

## 3. GUIDE D'INSTALLATION

### 3.1 Prérequis

```bash
# Vérifier les prérequis
docker --version        # >= 20.10
docker-compose --version # >= 1.29
git --version           # >= 2.30
python --version        # >= 3.9
```

### 3.2 Clone du Projet

```bash
# Cloner le dépôt
git clone https://github.com/ChristianMDG/MangaRivotra.git
cd MangaRivotra

# Voir la structure
tree -L 2
```

### 3.3 Configuration

```bash
# 1. Copier les variables d'environnement
cp .env.example .env

# 2. Éditer .env avec vos valeurs
nano .env
```

#### Fichier .env Example

```bash
# ============================================
# AIRFLOW CONFIGURATION
# ============================================
AIRFLOW_UID=50000
AIRFLOW_GID=50000

# PostgreSQL Airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# PostgreSQL Warehouse
WAREHOUSE_USER=warehouse
WAREHOUSE_PASSWORD=warehouse
WAREHOUSE_DB=air_quality_db

# Redis
REDIS_PASSWORD=redis

# ============================================
# API KEYS
# ============================================
OPENWEATHER_API_KEY=your_api_key_here

# ============================================
# AWS CONFIGURATION (si utilisation S3)
# ============================================
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_REGION=eu-west-1
S3_BUCKET=air-quality-datalake

# ============================================
# ENVIRONMENT
# ============================================
ENVIRONMENT=dev
FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=

# ============================================
# ALERTING
# ============================================
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
```

### 3.4 Installation et Démarrage

```bash
# 1. Build des images Docker
make setup

# 2. Démarrer tous les services
make up

# 3. Vérifier que tout fonctionne
docker-compose ps

# 4. Voir les logs
make logs
```

### 3.5 Accès aux Services

| Service | URL | Identifiant | Mot de passe |
|---------|-----|-------------|--------------|
| **Airflow UI** | http://localhost:8080 | admin | admin |
| **Superset** | http://localhost:8088 | admin | admin |
| **PgAdmin** | http://localhost:5050 | admin@airquality.com | admin |
| **MinIO UI** | http://localhost:9001 | minioadmin | minioadmin |
| **Grafana** | http://localhost:3000 | admin | admin |
| **Prometheus** | http://localhost:9090 | - | - |

---

## 4. STRUCTURE DU PROJET

### 4.1 Arborescence Détaillée

```
MangaRivotra/
│
├── docker/                          # Configuration Docker
│   ├── Dockerfile.airflow          # Image Airflow personnalisée
│   ├── docker-compose.yml          # Services complets
│   └── entrypoint.sh               # Script d'entrée Airflow
│
├── dags/                            # DAGs Airflow
│   ├── air_quality_pipeline.py     # DAG principal (VOUS)
│   ├── air_quality_backfill.py     # DAG historique (VOUS)
│   └── air_quality_monitoring.py   # DAG monitoring (VOUS)
│
├── plugins/                         # Plugins Airflow
│   ├── operators/                   # Opérateurs personnalisés (VOUS)
│   │   ├── extract_operator.py
│   │   ├── transform_operator.py
│   │   └── load_operator.py
│   └── sensors/                     # Capteurs personnalisés (VOUS)
│       └── api_sensor.py
│
├── scripts/                         # Scripts Python
│   ├── extract.py                  # Extraction (NOMENA)
│   ├── transform.py                # Transformation (MIHARINTSOA)
│   ├── load.py                     # Chargement (LUCAS)
│   └── quality_checks.py           # Qualité (MIHARINTSOA)
│
├── sql/                             # Requêtes SQL
│   ├── create_warehouse.sql        # Création tables (LUCAS)
│   ├── analytics_queries.sql       # Requêtes analyse (LUCAS)
│   └── migrations/                 # Migrations (LUCAS)
│
├── tests/                           # Tests
│   ├── test_dag.py                 # Tests DAG (VOUS)
│   ├── test_extract.py             # Tests extraction (NOMENA)
│   ├── test_transform.py           # Tests transformation (MIHARINTSOA)
│   └── test_quality.py             # Tests qualité (MIHARINTSOA)
│
├── notebooks/                       # Notebooks Jupyter
│   ├── 01_exploratory_analysis.ipynb  # Analyse exploratoire
│   ├── 02_visualization.ipynb         # Visualisations
│   └── 03_forecasting.ipynb           # Prédictions
│
├── data/                            # Données (locales)
│   ├── raw/                        # Données brutes JSON
│   ├── clean/                      # Données nettoyées Parquet
│   └── reports/                    # Rapports générés
│
├── config/                          # Configuration
│   ├── airflow.cfg                 # Config Airflow (VOUS)
│   └── connections.yaml            # Connexions (VOUS)
│
├── docs/                            # Documentation
│   ├── architecture.md             # Architecture (VOUS)
│   ├── setup_guide.md              # Guide installation (VOUS)
│   └── api_reference.md            # Référence API (NOMENA)
│
├── .github/                         # GitHub Actions
│   └── workflows/
│       └── ci_cd.yml               # CI/CD (VOUS)
│
├── .env.example                     # Variables d'environnement
├── .gitignore                       # Fichiers ignorés
├── requirements.txt                 # Dépendances Python
├── Makefile                         # Commandes make
├── setup.py                         # Setup package
└── README.md                        # Documentation principale
```

### 4.2 Fichiers Importants

#### requirements.txt
```txt
apache-airflow==2.7.0
apache-airflow-providers-postgres==5.6.0
apache-airflow-providers-http==4.5.0
apache-airflow-providers-amazon==7.4.0
pandas==2.1.0
numpy==1.24.3
pyarrow==13.0.0
psycopg2-binary==2.9.7
sqlalchemy==2.0.19
requests==2.31.0
great-expectations==0.17.10
pytest==7.4.0
matplotlib==3.7.2
seaborn==0.12.2
```

#### Makefile (Commandes Utiles)
```makefile
.PHONY: help setup up down restart logs clean test deploy

help:
	@echo "📋 Commandes disponibles :"
	@echo "  make setup    - Configurer l'environnement"
	@echo "  make up       - Démarrer les services"
	@echo "  make down     - Arrêter les services"
	@echo "  make logs     - Voir les logs"
	@echo "  make test     - Exécuter les tests"
	@echo "  make deploy   - Déployer en production"

setup:
	pip install -r requirements.txt
	cp .env.example .env
	docker-compose -f docker/docker-compose.yml build

up:
	docker-compose -f docker/docker-compose.yml up -d

down:
	docker-compose -f docker/docker-compose.yml down

logs:
	docker-compose -f docker/docker-compose.yml logs -f

test:
	pytest tests/ -v --cov=scripts

deploy:
	./scripts/deploy.sh
```

---

## 5. RÔLES ET RESPONSABILITÉS

### 5.1 Vous - Lead Architecte

```yaml
Responsabilités:
  - Architecture globale du système
  - Configuration d'Airflow en production
  - Intégration de tous les composants
  - CI/CD et déploiement
  - Coordination d'équipe
  - Documentation finale

Fichiers à gérer:
  ✅ docker/docker-compose.yml
  ✅ dags/air_quality_pipeline.py
  ✅ plugins/operators/*.py
  ✅ .github/workflows/ci_cd.yml
  ✅ docs/architecture.md
  ✅ README.md

Commandes principales:
  make up      # Démarrer l'infrastructure
  make deploy  # Déployer en production
  make test    # Lancer les tests

Livrables:
  - ✅ Infrastructure Docker fonctionnelle
  - ✅ DAG Airflow opérationnel
  - ✅ CI/CD configuré
  - ✅ Documentation complète
  - ✅ Vidéo (intro + conclusion)
```

### 5.2 Nomena - Data Engineer (Extraction)

```yaml
Responsabilités:
  - Intégration de l'API OpenWeather
  - Scripts d'extraction
  - Gestion du Data Lake (S3/MinIO)
  - Gestion des erreurs d'API
  - Gestion des données historiques

Fichiers à gérer:
  ✅ scripts/extract.py
  ✅ scripts/extract_historical.py
  ✅ tests/test_extract.py
  ✅ docs/api_reference.md

Commandes principales:
  python scripts/extract.py --city Paris
  python scripts/extract_historical.py --start 2023-09-01

Livrables:
  - ✅ Extraction des 5 villes fonctionnelle
  - ✅ Données stockées dans S3/MinIO
  - ✅ Gestion des erreurs robuste
  - ✅ Tests d'extraction passent
  - ✅ Documentation extraction à jour
  - ✅ Vidéo (section extraction)
```

### 5.3 Miharintsoa - Data Engineer (Transformation)

```yaml
Responsabilités:
  - Nettoyage des données brutes
  - Enrichissement des données
  - Contrôle de qualité des données
  - Gestion des valeurs aberrantes

Fichiers à gérer:
  ✅ scripts/transform.py
  ✅ scripts/quality_checks.py
  ✅ tests/test_transform.py
  ✅ tests/test_quality.py

Commandes principales:
  python scripts/transform.py --input data/raw/ --output data/clean/
  python scripts/quality_checks.py --data data/clean/

Livrables:
  - ✅ Transformation fonctionnelle
  - ✅ Qualité des données vérifiée
  - ✅ Features dérivées ajoutées
  - ✅ Tests de qualité passent
  - ✅ Documentation transformation à jour
  - ✅ Vidéo (section transformation)
```

### 5.4 Lucas - Data Analyst / BI

```yaml
Responsabilités:
  - Modélisation des données (Star Schema)
  - Configuration du Data Warehouse
  - Création des dashboards
  - Requêtes d'analyse

Fichiers à gérer:
  ✅ sql/create_warehouse.sql
  ✅ sql/analytics_queries.sql
  ✅ scripts/load.py
  ✅ Superset Dashboard

Commandes principales:
  psql -U warehouse -d air_quality_db -f sql/create_warehouse.sql
  python scripts/load.py --data data/clean/

Livrables:
  - ✅ Data Warehouse créé
  - ✅ Modèle en étoile implémenté
  - ✅ Dashboard fonctionnel
  - ✅ Requêtes d'analyse testées
  - ✅ Documentation BI à jour
  - ✅ Vidéo (section dashboard)
```

---

## 6. GUIDE DE DÉVELOPPEMENT

### 6.1 Workflow Git

```bash
# 1. Créer une branche pour votre feature
git checkout -b feature/extraction-nomena
# ou
git checkout -b feature/transformation-miharintsoa
# ou
git checkout -b feature/warehouse-lucas

# 2. Développer et commit
git add .
git commit -m "Ajout de la fonction d'extraction pour Paris"

# 3. Pousser vers GitHub
git push origin feature/extraction-nomena

# 4. Créer une Pull Request sur GitHub
# Aller sur GitHub > Pull Requests > New Pull Request
```

### 6.2 Standards de Code

#### Python (PEP 8)
```python
# ✅ Bon exemple
def extract_city_data(city_name: str, api_key: str) -> Dict[str, Any]:
    """
    Extrait les données de qualité de l'air pour une ville.
    
    Args:
        city_name: Nom de la ville
        api_key: Clé API OpenWeather
    
    Returns:
        Dict contenant les données
    """
    return {"city": city_name, "status": "success"}

# ❌ Mauvais exemple
def extract(city, key):
    return {"city": city}
```

#### SQL
```sql
-- ✅ Bon exemple
SELECT 
    c.city_name,
    AVG(f.aqi) AS avg_aqi
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
GROUP BY c.city_name;

-- ❌ Mauvais exemple
SELECT c.city_name, AVG(f.aqi) FROM fact_air_quality f, dim_city c WHERE f.city_id = c.city_id GROUP BY c.city_name;
```

### 6.3 Tests

```bash
# Exécuter tous les tests
make test

# Exécuter un test spécifique
pytest tests/test_extract.py -v

# Exécuter avec couverture
pytest tests/ --cov=scripts --cov-report=html

# Voir le rapport de couverture
open htmlcov/index.html
```

### 6.4 Logs et Débogage

```bash
# Voir tous les logs
make logs

# Voir les logs d'un service spécifique
docker-compose logs -f webserver
docker-compose logs -f scheduler
docker-compose logs -f worker

# Voir les logs Airflow
docker-compose exec webserver cat logs/air_quality_pipeline/*.log

# Voir les logs PostgreSQL
docker-compose logs postgres_warehouse
```

---

## 7. GUIDE D'UTILISATION

### 7.1 Utilisation d'Airflow

#### Accès à l'UI
```
http://localhost:8080
Identifiant: admin
Mot de passe: admin
```

#### Actions dans Airflow

```bash
# Activer/Désactiver un DAG
# Dans l'UI: cliquer sur le toggle à côté du nom du DAG

# Déclencher manuellement un DAG
# Dans l'UI: cliquer sur le bouton "▶" (Trigger DAG)

# Ou en ligne de commande
docker-compose exec webserver airflow dags trigger air_quality_pipeline

# Voir les logs d'une tâche
docker-compose exec webserver airflow tasks logs air_quality_pipeline extract_paris 2024-01-15
```

### 7.2 Utilisation des Scripts

#### Extraction (Nomena)
```bash
# Extraire une ville spécifique
python scripts/extract.py --city Paris --api-key VOTRE_CLE

# Extraire toutes les villes
python scripts/extract.py --all-cities

# Extraire avec historique
python scripts/extract_historical.py --start 2023-09-01 --end 2024-01-15
```

#### Transformation (Miharintsoa)
```bash
# Transformer les données brutes
python scripts/transform.py --input data/raw/ --output data/clean/

# Vérifier la qualité des données
python scripts/quality_checks.py --data data/clean/ --report html
```

#### Chargement (Lucas)
```bash
# Charger dans le Data Warehouse
python scripts/load.py --data data/clean/ --warehouse postgresql://...

# Exécuter les requêtes d'analyse
psql -U warehouse -d air_quality_db -f sql/analytics_queries.sql
```

### 7.3 Dashboard Superset

#### Accès
```
http://localhost:8088
Identifiant: admin
Mot de passe: admin
```

#### Création d'un Dashboard

1. **Se connecter à Superset**
2. **Connecter la source de données** : PostgreSQL
3. **Créer un Dataset** : `fact_air_quality`
4. **Créer des Charts** :
   - Bar Chart: AQI par ville
   - Line Chart: Évolution AQI
   - Pie Chart: Répartition des polluants
5. **Créer un Dashboard** et y ajouter les charts

---

## 8. TESTS ET QUALITÉ

### 8.1 Types de Tests

```python
# tests/test_extract.py - Test d'extraction
def test_extract_paris():
    result = extract_air_quality_data("Paris", "test_key", "/tmp")
    assert result["status"] == "success"
    assert result["city"] == "Paris"

# tests/test_transform.py - Test de transformation
def test_transform_data():
    df = pd.DataFrame({"city_name": ["Paris"], "aqi": [50]})
    cleaned = clean_data(df)
    assert len(cleaned) > 0
    assert "aqi_category" in cleaned.columns

# tests/test_quality.py - Test de qualité
def test_quality_checks():
    results = run_quality_checks("data/clean/")
    assert results["status"] == "pass"
    assert len(results["checks"]) > 0
```

### 8.2 Qualité des Données

#### Vérifications Automatiques

```python
# scripts/quality_checks.py

def run_quality_checks(data_path):
    checks = {
        'row_count': {'min': 5},
        'missing_values': {'max_percent': 10},
        'duplicates': {'max': 0},
        'value_ranges': {
            'aqi': (0, 500),
            'pm2_5': (0, 500),
            'pm10': (0, 1000)
        },
        'cities_presence': ['Paris', 'London', 'Berlin', 'Madrid', 'Rome']
    }
    # ... exécution des vérifications
```

### 8.3 Rapports de Qualité

```bash
# Générer un rapport HTML
python scripts/quality_checks.py --data data/clean/ --report html

# Générer un rapport JSON
python scripts/quality_checks.py --data data/clean/ --report json
```

---

## 9. DÉPLOIEMENT

### 9.1 Environnement de Développement

```bash
# 1. Build et démarrage
make up

# 2. Vérification
curl http://localhost:8080/health
curl http://localhost:8088/health

# 3. Tests
make test
```

### 9.2 Environnement de Production

```bash
# 1. Déploiement complet
make deploy

# 2. Vérification
docker-compose ps
curl http://localhost:8080/health

# 3. Monitoring
docker-compose logs -f
```

### 9.3 Script de Déploiement Automatisé

```bash
#!/bin/bash
# scripts/deploy.sh

set -e

echo "🚀 Déploiement du Pipeline de Qualité de l'Air"

# Vérification des prérequis
command -v docker >/dev/null 2>&1 || { echo "❌ Docker requis"; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "❌ Docker Compose requis"; exit 1; }

# Configuration
cp .env.example .env
echo "⚠️ Veuillez configurer le fichier .env"

# Build
docker-compose -f docker/docker-compose.yml build

# Démarrage
docker-compose -f docker/docker-compose.yml up -d

# Vérification
sleep 10
curl -s http://localhost:8080/health > /dev/null

echo "✅ Déploiement terminé avec succès !"
```

---

## 10. FAQ ET DÉPANNAGE

### 10.1 Problèmes Courants

#### Problème: Airflow ne démarre pas

```bash
# Vérifier les logs
docker-compose logs webserver

# Solution: Réinitialiser la base de données
docker-compose down -v
docker-compose up -d

# Réinitialiser Airflow
docker-compose exec webserver airflow db reset
docker-compose exec webserver airflow db upgrade
```

#### Problème: API OpenWeather ne répond pas

```bash
# Vérifier la clé API
echo $OPENWEATHER_API_KEY

# Tester manuellement
curl "http://api.openweathermap.org/data/2.5/air_pollution?lat=48.8566&lon=2.3522&appid=VOTRE_CLE"

# Solution: Vérifier le quota API
# Aller sur https://openweathermap.org/api/air-pollution
```

#### Problème: PostgreSQL ne démarre pas

```bash
# Voir les logs
docker-compose logs postgres_warehouse

# Vérifier le port
netstat -tulpn | grep 5433

# Solution: Changer le port dans docker-compose.yml
ports:
  - "5434:5432"  # Changement de port
```

#### Problème: Permission Docker

```bash
# Solution 1: Ajouter l'utilisateur au groupe docker
sudo usermod -aG docker $USER
# Redémarrer la session

# Solution 2: Utiliser sudo
sudo docker-compose up -d
```

### 10.2 Commandes de Dépannage

```bash
# Voir tous les conteneurs
docker-compose ps

# Voir les logs d'un conteneur spécifique
docker-compose logs -f webserver

# Entrer dans un conteneur
docker-compose exec webserver bash
docker-compose exec postgres_warehouse psql -U warehouse

# Vérifier les données dans PostgreSQL
docker-compose exec postgres_warehouse psql -U warehouse -d air_quality_db -c "SELECT COUNT(*) FROM fact_air_quality;"

# Vérifier les fichiers dans MinIO
docker-compose exec minio mc ls local/air-quality-datalake/

# Réinitialiser complètement
docker-compose down -v
docker-compose up -d
```

### 10.3 Monitoring

#### URL de Monitoring

- **Airflow Health**: http://localhost:8080/health
- **Airflow Metrics**: http://localhost:8080/metrics
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000

#### Alertes Configurées

```yaml
Alertes Slack:
  - Échec de tâche Airflow
  - Qualité des données < 90%
  - AQI > 150 (alerte pollution)
  - Pipeline en retard > 1h
```

---

## 11. ANNEXES

### 11.1 Référence API OpenWeather

```python
# Endpoint: Air Pollution
GET http://api.openweathermap.org/data/2.5/air_pollution

# Paramètres
{
    "lat": 48.8566,     # Latitude
    "lon": 2.3522,      # Longitude
    "appid": "VOTRE_CLE" # Clé API
}

# Réponse
{
    "coord": {
        "lon": 2.3522,
        "lat": 48.8566
    },
    "list": [{
        "dt": 1605182400,
        "main": {
            "aqi": 2
        },
        "components": {
            "co": 201.94,
            "no": 0.02,
            "no2": 0.15,
            "o3": 42.09,
            "pm2_5": 4.56,
            "pm10": 8.12,
            "so2": 0.47,
            "nh3": 0.52
        }
    }]
}
```

### 11.2 Structure des Données

#### Schéma JSON Brut
```json
{
    "city_name": "Paris",
    "country": "FR",
    "extraction_timestamp": "2024-01-15T10:30:00",
    "coord": {"lon": 2.3522, "lat": 48.8566},
    "list": [
        {
            "dt": 1705300000,
            "main": {"aqi": 2},
            "components": {
                "co": 201.94,
                "no": 0.02,
                "no2": 0.15,
                "o3": 42.09,
                "pm2_5": 4.56,
                "pm10": 8.12,
                "so2": 0.47,
                "nh3": 0.52
            }
        }
    ]
}
```

#### Schéma Parquet Nettoyé
```python
columns = [
    'city_name', 'country', 'extraction_timestamp',
    'aqi', 'co', 'no', 'no2', 'o3', 'pm2_5', 'pm10', 'so2', 'nh3',
    'dt', 'dt_datetime', 'date', 'hour', 'day_of_week', 'month', 'year',
    'is_weekend', 'aqi_category', 'pm25_pm10_ratio', 'season'
]
```

### 11.3 Liens Utiles

#### Documentation
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [OpenWeather API](https://openweathermap.org/api/air-pollution)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Superset Documentation](https://superset.apache.org/docs/)
- [Docker Documentation](https://docs.docker.com/)

#### Outils
- [GitHub](https://github.com/yourteam/MangaRivotra)
- [Docker Hub](https://hub.docker.com/)
- [Python Package Index](https://pypi.org/)

#### Support
- [Airflow Slack](https://apache-airflow.slack.com/)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/airflow)
- [OpenWeather Forum](https://openweathermap.org/forum)

---

## 📋 CHECKLIST FINALE ÉQUIPE

### VOUS (Lead)
- [ ] Docker Compose configuré
- [ ] DAG Airflow opérationnel
- [ ] CI/CD configuré
- [ ] Documentation complète
- [ ] Vidéo (intro + conclusion