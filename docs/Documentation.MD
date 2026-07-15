# 🌍 Projet IAI - Pipeline Qualité de l'Air

## Documentation Complète du Projet

---

## 📋 TABLE DES MATIÈRES

1. [Présentation du Projet](#1-présentation-du-projet)
2. [Architecture Technique](#2-architecture-technique)
3. [Guide d'Installation](#3-guide-dinstallation)
4. [Structure du Projet](#4-structure-du-projet)
5. [Rôles et Responsabilités](#5-rôles-et-responsabilités)
6. [Guide de Développement](#6-guide-de-développement)
7. [Contrat de Données - clean/](#7-contrat-de-données---clean)
8. [Modélisation du Data Warehouse](#8-modélisation-du-data-warehouse)
9. [Tests et Qualité](#9-tests-et-qualité)
10. [Déploiement](#10-déploiement)
11. [FAQ et Dépannage](#11-faq-et-dépannage)
12. [Rapport de Projet](#12-rapport-de-projet)

---

## 1. PRÉSENTATION DU PROJET

### 1.1 Objectif

Déployer un pipeline automatisé qui collecte 24h/24 des données de qualité de l'air (AQI) pour **5 villes minimum**, et livrer un **Data Warehouse** basé sur une modélisation dimensionnelle (étoile ou flocon).

**Le pipeline doit continuer à tourner après le rendu** : le cours IA1 consommera vos données au fil de l'eau.

### 1.2 Équipe Projet

| Membre | Rôle | Responsabilités |
|--------|------|-----------------|
| **Vous** | Lead Architecte | Architecture, Orchestration, CI/CD, Documentation |
| **Nomena** | Data Engineer | Extraction API, Gestion raw/, Backfill |
| **Miharintsoa** | Data Engineer | Transformation, clean/, Qualité données |
| **Lucas** | Data Analyst / BI | Data Warehouse, Modélisation, Scripts SQL |
| **[Membre 5]** | Data Engineer | Tests, Validation, Support |

### 1.3 Villes Choisies

| Ville | Pays | Latitude | Longitude |
|-------|------|----------|-----------|
| **Paris** | France | 48.8566 | 2.3522 |
| **London** | Royaume-Uni | 51.5074 | -0.1278 |
| **Berlin** | Allemagne | 52.5200 | 13.4050 |
| **Madrid** | Espagne | 40.4168 | -3.7038 |
| **Rome** | Italie | 41.9028 | 12.4964 |

---

## 2. ARCHITECTURE TECHNIQUE

### 2.1 Architecture Globale

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PIPELINE DE QUALITÉ DE L'AIR - IAI                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         API OpenWeather                              │  │
│  │                  (Qualité de l'air - 5 villes)                       │  │
│  └────────────────────────────┬─────────────────────────────────────────┘  │
│                               │                                             │
│                               ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    ORCHESTRATEUR (Airflow)                           │  │
│  │  ┌──────────────────────────────────────────────────────────────┐   │  │
│  │  │  DAG: air_quality_pipeline (Exécution horaire)              │   │  │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐      │   │  │
│  │  │  │ Extract │─▶│Transform│─▶│ Quality │─▶│  Load   │      │   │  │
│  │  │  │ (5 villes)│  │ (Clean) │  │  Check  │  │  DW     │      │   │  │
│  │  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘      │   │  │
│  │  └──────────────────────────────────────────────────────────────┘   │  │
│  └────────────────────────────┬─────────────────────────────────────────┘  │
│                               │                                             │
│                               ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         STOCKAGE                                     │  │
│  │  ┌────────────────────────────┐  ┌────────────────────────────┐    │  │
│  │  │  raw/ (INTouchable)        │  │  clean/ (Reconstruit)      │    │  │
│  │  │  └── city/date/*.json      │  │  └── air_quality.csv       │    │  │
│  │  │  Sauvegarde brute          │  │  Fichier unique, propre    │    │  │
│  │  └────────────────────────────┘  └────────────────────────────┘    │  │
│  └────────────────────────────┬─────────────────────────────────────────┘  │
│                               │                                             │
│                               ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                     DATA WAREHOUSE (PostgreSQL)                      │  │
│  │  ┌──────────────────────────────────────────────────────────────┐   │  │
│  │  │  Modèle en Étoile                                            │   │  │
│  │  │  ┌─────────────┐     ┌──────────────────────────────────┐   │   │  │
│  │  │  │ dim_city    │────▶│          fact_air_quality        │   │   │  │
│  │  │  │ city_id PK  │     │  city_id FK, time_id FK         │   │   │  │
│  │  │  │ city_name   │     │  aqi, co, no2, pm2_5, pm10, etc │   │   │  │
│  │  │  │ country     │     └──────────────┬───────────────────┘   │   │  │
│  │  │  │ lat, lon    │                    │                       │   │  │
│  │  │  └─────────────┘                    │                       │   │  │
│  │  │                              ┌──────┴──────────┐            │   │  │
│  │  │                              │    dim_time     │            │   │  │
│  │  │                              │  time_id PK     │            │   │  │
│  │  │                              │  date, hour     │            │   │  │
│  │  │                              │  day_of_week    │            │   │  │
│  │  │                              │  is_weekend     │            │   │  │
│  │  │                              └─────────────────┘            │   │  │
│  │  └──────────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Stack Technologique

| Composant | Technologie | Version | Justification |
|-----------|-------------|---------|---------------|
| **Orchestration** | Apache Airflow | 2.7.0 | Standard de l'industrie, monitoring intégré, UI pour suivi des exécutions |
| **Stockage** | MinIO (dev) / AWS S3 (prod) | Latest | Compatible S3, persistance des données brutes et nettoyées |
| **Data Warehouse** | PostgreSQL | 14 | Robuste, open-source, supporte bien les modèles dimensionnels |
| **Traitement** | Python + Pandas | 3.9 | Flexible, large écosystème pour ETL |
| **Conteneurisation** | Docker + Docker Compose | Latest | Isolation, reproductibilité, déploiement facile |
| **Code** | Git + GitHub | - | Versionnement, collaboration, CI/CD |

### 2.3 Justification des Choix Techniques

#### Airflow vs Alternatives (Cron, Lambda, Dagster)
- ✅ **Airflow** : Monitoring intégré, UI pour suivre les exécutions, gestion des erreurs, retry automatique
- ✅ **Historique d'exécutions** : Preuve que le pipeline tourne automatiquement
- ✅ **DAG visual** : Tâches et dépendances claires

#### Stockage S3/MinIO
- ✅ **raw/ intouchable** : Partitionnement par ville/date pour sauvegarde
- ✅ **clean/ unique** : Fichier CSV unique, facile à partager avec IA1

#### PostgreSQL
- ✅ **Modélisation dimensionnelle** : Support natif des schémas en étoile
- ✅ **Accès simple** : Connexion directe pour IA1

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

### 3.2 Installation

```bash
# 1. Cloner le projet
git clone https://github.com/yourteam/air-quality-pipeline.git
cd air-quality-pipeline

# 2. Copier les variables d'environnement
cp .env.example .env

# 3. Éditer .env avec votre clé API
nano .env
# Ajouter: OPENWEATHER_API_KEY=votre_cle_api

# 4. Build des images Docker
make setup

# 5. Démarrer les services
make up
```

### 3.3 Accès aux Services

| Service | URL | Identifiant | Mot de passe |
|---------|-----|-------------|--------------|
| **Airflow UI** | http://localhost:8080 | admin | admin |
| **PgAdmin** | http://localhost:5050 | admin@airquality.com | admin |
| **MinIO UI** | http://localhost:9001 | minioadmin | minioadmin |

---

## 4. STRUCTURE DU PROJET

```
air-quality-pipeline/
│
├── docker/
│   ├── Dockerfile.airflow          # Image Airflow
│   └── docker-compose.yml          # Services complets
│
├── dags/
│   ├── air_quality_pipeline.py     # DAG principal (Airflow)
│   └── air_quality_backfill.py     # DAG pour backfill
│
├── scripts/
│   ├── extract.py                  # Extraction API (Nomena)
│   ├── transform.py                # Transformation -> clean/ (Miharintsoa)
│   ├── load_warehouse.py           # Chargement DW (Lucas)
│   └── validate_clean.py           # Validation clean/ (Membre 5)
│
├── sql/
│   ├── create_dw.sql               # Création Data Warehouse
│   ├── queries_analytics.sql       # Requêtes d'analyse
│   └── check_coherence.sql         # Vérification cohérence
│
├── data/
│   ├── raw/                        # DONNÉES BRUTES (intouchables)
│   │   └── air_quality/
│   │       ├── paris/YYYY-MM-DD/*.json
│   │       ├── london/YYYY-MM-DD/*.json
│   │       └── ...
│   └── clean/                      # DONNÉES NETTOYÉES
│       └── air_quality_YYYYMMDD_HHMMSS.csv  # Fichier unique
│
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
│
├── docs/
│   ├── ARCHITECTURE.md             # Documentation architecture
│   └── README_STORAGE.md           # Documentation stockage
│
├── .env.example                     # Variables d'environnement
├── requirements.txt                 # Dépendances Python
├── Makefile                         # Commandes make
└── README.md                        # Documentation principale
```

---

## 5. RÔLES ET RESPONSABILITÉS

### 5.1 Vous - Lead Architecte

```yaml
Responsabilités:
  - Architecture globale du système
  - Configuration d'Airflow
  - Orchestration du pipeline (DAG)
  - CI/CD et déploiement
  - Coordination d'équipe
  - ARCHITECTURE.md

Fichiers à gérer:
  ✅ docker/docker-compose.yml
  ✅ dags/air_quality_pipeline.py
  ✅ docs/ARCHITECTURE.md
  ✅ .github/workflows/ci_cd.yml
  ✅ README.md

Commandes principales:
  make up      # Démarrer l'infrastructure
  make logs    # Voir les logs

Livrables:
  - Infrastructure Docker fonctionnelle
  - DAG Airflow opérationnel
  - ARCHITECTURE.md complet
  - Vidéo (parties architecture + orchestration)
```

### 5.2 Nomena - Data Engineer (Extraction)

```yaml
Responsabilités:
  - Intégration API OpenWeather
  - Script d'extraction
  - Gestion raw/ (stockage JSON)
  - Backfill 12 mois
  - Gestion des secrets

Fichiers à gérer:
  ✅ scripts/extract.py
  ✅ scripts/extract_backfill.py
  ✅ tests/test_extract.py
  ✅ data/raw/ (données)

Commandes principales:
  python scripts/extract.py --city Paris
  python scripts/extract_backfill.py --start 2023-01-01

Livrables:
  - Extraction 5 villes fonctionnelle
  - raw/ rempli (12 mois)
  - Gestion d'erreurs
  - tests/extract.py passent
  - Vidéo (section extraction + raw/)
```

### 5.3 Miharintsoa - Data Engineer (Transformation)

```yaml
Responsabilités:
  - Nettoyage des données
  - Génération de clean/air_quality.csv
  - Validation du contrat de données
  - Gestion de la déduplication

Fichiers à gérer:
  ✅ scripts/transform.py
  ✅ scripts/validate_clean.py
  ✅ tests/test_transform.py
  ✅ data/clean/ (fichier CSV)

Commandes principales:
  python scripts/transform.py
  python scripts/validate_clean.py --file data/clean/air_quality.csv

Livrables:
  - clean/air_quality.csv unique
  - Contrat de données respecté
  - Déduplication horaire
  - tests/transform.py passent
  - Vidéo (section transformation + clean/)
```

### 5.4 Lucas - Data Analyst / BI

```yaml
Responsabilités:
  - Modélisation (étoile ou flocon)
  - Scripts SQL du Data Warehouse
  - Script load_warehouse.py
  - Cohérence des données

Fichiers à gérer:
  ✅ sql/create_dw.sql
  ✅ sql/check_coherence.sql
  ✅ scripts/load_warehouse.py
  ✅ docs/README_STORAGE.md (schéma DW)

Commandes principales:
  psql -U warehouse -d air_quality_db -f sql/create_dw.sql
  python scripts/load_warehouse.py

Livrables:
  - Data Warehouse créé
  - Modèle en étoile ou flocon
  - load_warehouse.py rejouable
  - Cohérence vérifiée
  - Vidéo (section Data Warehouse + requêtes)
```

### 5.5 Membre 5 - Data Quality / Tests

```yaml
Responsabilités:
  - Validation du contrat de données
  - Tests d'intégration
  - Validation clean/
  - Cohérence entre raw/ et clean/

Fichiers à gérer:
  ✅ scripts/validate_clean.py
  ✅ tests/test_integration.py
  ✅ docs/README_STORAGE.md (validation)

Commandes principales:
  python scripts/validate_clean.py
  python tests/test_integration.py

Livrables:
  - Script validation clean/
  - Tests d'intégration
  - Rapport de qualité
  - Vidéo (section validation)
```

---

## 6. GUIDE DE DÉVELOPPEMENT

### 6.1 Workflow Git

```bash
# 1. Créer une branche
git checkout -b feature/extraction-nomena

# 2. Développer
# ... code ...

# 3. Commit
git add .
git commit -m "Feat: extraction Paris fonctionnelle"

# 4. Push
git push origin feature/extraction-nomena

# 5. Pull Request sur GitHub
```

### 6.2 Standards de Code

#### Python (PEP 8)
```python
# ✅ Bon exemple
def extract_city_data(city_name: str, api_key: str) -> Dict[str, Any]:
    """Extrait les données pour une ville."""
    return {"city": city_name, "status": "success"}

# ❌ Mauvais exemple
def extract(city, key):
    return {"city": city}
```

#### Nommage des Fichiers
```bash
# raw/
raw/air_quality/paris/2024-01-15/paris_20240115_120000.json

# clean/
clean/air_quality_20240115_120000.csv
```

---

## 7. CONTRAT DE DONNÉES - clean/

### 7.1 Format du Fichier

**Fichier** : `clean/air_quality.csv` (unique, toutes villes réunies)

**Format** : CSV avec séparateur `,` et en-têtes

**Règles** :
- Une ligne par ville et par heure
- Trié chronologiquement
- Pas de doublons (même ville + même heure = 1 ligne)
- Aucune valeur manquante pour les colonnes critiques

### 7.2 Colonnes et Unités

| Colonne | Type | Description | Unité |
|---------|------|-------------|-------|
| `city` | STRING | Nom de la ville | - |
| `country` | STRING | Code pays (FR, UK, DE, ES, IT) | - |
| `latitude` | FLOAT | Latitude | degrés |
| `longitude` | FLOAT | Longitude | degrés |
| `timestamp` | TIMESTAMP | Horodatage de la mesure | UTC |
| `date` | DATE | Date de la mesure (YYYY-MM-DD) | - |
| `hour` | INTEGER | Heure (0-23) | - |
| `aqi` | INTEGER | Indice de qualité de l'air (1-5) | AQI |
| `co` | FLOAT | Monoxyde de carbone | μg/m³ |
| `no` | FLOAT | Monoxyde d'azote | μg/m³ |
| `no2` | FLOAT | Dioxyde d'azote | μg/m³ |
| `o3` | FLOAT | Ozone | μg/m³ |
| `pm2_5` | FLOAT | Particules fines <2.5μm | μg/m³ |
| `pm10` | FLOAT | Particules fines <10μm | μg/m³ |
| `so2` | FLOAT | Dioxyde de soufre | μg/m³ |
| `nh3` | FLOAT | Ammoniac | μg/m³ |

### 7.3 Exemple de Données

```csv
city,country,latitude,longitude,timestamp,date,hour,aqi,co,no,no2,o3,pm2_5,pm10,so2,nh3
Paris,FR,48.8566,2.3522,2024-01-15 10:00:00,2024-01-15,10,2,201.94,0.02,0.15,42.09,4.56,8.12,0.47,0.52
London,UK,51.5074,-0.1278,2024-01-15 10:00:00,2024-01-15,10,3,150.32,0.03,0.21,35.67,8.23,12.45,0.62,0.38
Berlin,DE,52.5200,13.4050,2024-01-15 10:00:00,2024-01-15,10,2,180.45,0.01,0.12,48.23,3.45,6.78,0.35,0.45
```

### 7.4 Script de Validation

```python
# scripts/validate_clean.py

def validate_clean_file(filepath: str) -> bool:
    """
    Valide le fichier clean/air_quality.csv
    """
    df = pd.read_csv(filepath)
    
    # 1. Colonnes obligatoires
    required_cols = ['city', 'timestamp', 'aqi', 'pm2_5', 'pm10']
    assert all(col in df.columns for col in required_cols)
    
    # 2. Pas de valeurs manquantes critiques
    assert df[required_cols].notna().all().all()
    
    # 3. Pas de doublons
    assert not df.duplicated(subset=['city', 'timestamp']).any()
    
    # 4. AQI dans [1, 5]
    assert df['aqi'].between(1, 5).all()
    
    # 5. PM2.5 >= 0
    assert (df['pm2_5'] >= 0).all()
    assert (df['pm10'] >= 0).all()
    
    return True
```

---

## 8. MODÉLISATION DU DATA WAREHOUSE

### 8.1 Schéma en Étoile

```
┌─────────────────────────────────────────────────────────────────┐
│                      SCHÉMA EN ÉTOILE                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│                    ┌──────────────────┐                         │
│                    │    dim_city      │                         │
│                    ├──────────────────┤                         │
│                    │ city_id (PK)     │                         │
│                    │ city_name        │                         │
│                    │ country          │                         │
│                    │ latitude         │                         │
│                    │ longitude        │                         │
│                    └────────┬─────────┘                         │
│                             │                                    │
│                             │                                    │
│  ┌──────────────────┐      │       ┌──────────────────────────┐│
│  │    dim_time      │      │       │   fact_air_quality       ││
│  ├──────────────────┤      │       ├──────────────────────────┤│
│  │ time_id (PK)     │──────┼───────│ fact_id (PK)             ││
│  │ date             │      │       │ city_id (FK)             ││
│  │ hour             │      │       │ time_id (FK)             ││
│  │ day_of_week      │      │       │ aqi                      ││
│  │ month            │      │       │ co                       ││
│  │ year             │      │       │ no                       ││
│  │ is_weekend       │      │       │ no2                      ││
│  │ is_holiday       │      │       │ o3                       ││
│  └──────────────────┘      │       │ pm2_5                    ││
│                             │       │ pm10                     ││
│                             │       │ so2                      ││
│                             │       │ nh3                      ││
│                             │       │ measurement_timestamp   ││
│                             │       └──────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘

Justification du schéma en étoile:
- Simplicité: 1 table de faits + 2 dimensions
- Performances: jointures simples
- Adapté au volume de données (5 villes × 8760 heures = ~43 800 lignes par an)
```

### 8.2 Scripts SQL

```sql
-- sql/create_dw.sql

-- ============================================
-- DIMENSION CITY
-- ============================================
CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(50),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- DIMENSION TIME
-- ============================================
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    day_of_week VARCHAR(20),
    day_of_week_num INTEGER,
    month INTEGER,
    year INTEGER,
    is_weekend BOOLEAN DEFAULT FALSE,
    UNIQUE(date, hour)
);

CREATE INDEX idx_dim_time_date ON dim_time(date);
CREATE INDEX idx_dim_time_month ON dim_time(month, year);

-- ============================================
-- TABLE FAIT - AIR QUALITY
-- ============================================
CREATE TABLE IF NOT EXISTS fact_air_quality (
    fact_id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES dim_city(city_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    aqi INTEGER,
    co DECIMAL(10,2),
    no DECIMAL(10,2),
    no2 DECIMAL(10,2),
    o3 DECIMAL(10,2),
    pm2_5 DECIMAL(10,2),
    pm10 DECIMAL(10,2),
    so2 DECIMAL(10,2),
    nh3 DECIMAL(10,2),
    measurement_timestamp TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city_id, time_id)
);

-- Index pour performances
CREATE INDEX idx_fact_city ON fact_air_quality(city_id);
CREATE INDEX idx_fact_time ON fact_air_quality(time_id);
CREATE INDEX idx_fact_aqi ON fact_air_quality(aqi);
```

### 8.3 Script de Chargement

```python
# scripts/load_warehouse.py

def load_to_warehouse(clean_file: str):
    """Charge les données clean/ dans le Data Warehouse"""
    
    # Lecture CSV
    df = pd.read_csv(clean_file)
    
    with engine.connect() as conn:
        # 1. Chargement dim_city
        for city in df['city'].unique():
            # UPSERT ville
            ...
        
        # 2. Chargement dim_time
        for date in df['date'].unique():
            # UPSERT temps
            ...
        
        # 3. Chargement fact_air_quality
        for _, row in df.iterrows():
            # INSERT UNIQUE
            ...
```

### 8.4 Cohérence Attendue

```sql
-- sql/check_coherence.sql

-- Vérification: nombre de lignes ≈ villes × heures
SELECT 
    COUNT(*) AS total_facts,
    (SELECT COUNT(DISTINCT city_id) FROM dim_city) * 
    (SELECT COUNT(DISTINCT time_id) FROM dim_time) AS expected_max
FROM fact_air_quality;

-- Vérification des trous
SELECT 
    c.city_name,
    COUNT(*) AS nb_mesures,
    MIN(t.date) AS min_date,
    MAX(t.date) AS max_date
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY c.city_name;
```

---

## 9. TESTS ET QUALITÉ

### 9.1 Validation clean/

```python
# tests/test_clean.py

def test_clean_file():
    """Valide le fichier clean/"""
    df = pd.read_csv('data/clean/air_quality.csv')
    
    # 1. Structure
    assert 'city' in df.columns
    assert 'timestamp' in df.columns
    assert 'aqi' in df.columns
    
    # 2. Pas de doublons
    assert not df.duplicated(['city', 'timestamp']).any()
    
    # 3. Données cohérentes
    assert df['aqi'].between(1, 5).all()
    assert (df['pm2_5'] >= 0).all()
    
    # 4. Villes présentes
    expected_cities = ['Paris', 'London', 'Berlin', 'Madrid', 'Rome']
    assert all(city in df['city'].unique() for city in expected_cities)
```

### 9.2 Backfill Validation

```bash
# Vérifier que raw/ contient 12 mois de données
python scripts/validate_backfill.py --start 2023-01-01 --end 2024-01-01

# Vérifier que clean/ contient toutes les données
python scripts/validate_clean.py --file data/clean/air_quality.csv
```

---

## 10. DÉPLOIEMENT

### 10.1 Démarrage

```bash
# 1. Démarrer les services
make up

# 2. Vérifier Airflow
curl http://localhost:8080/health

# 3. Activer le DAG
# Dans Airflow UI, activer air_quality_pipeline
```

### 10.2 Vérification Continue

```bash
# Vérifier que le DAG tourne
docker-compose logs -f scheduler | grep "air_quality_pipeline"

# Voir les exécutions dans Airflow UI
# http://localhost:8080/dag/air_quality_pipeline/grid

# Vérifier les données
docker-compose exec postgres_warehouse psql -U warehouse -d air_quality_db \
  -c "SELECT COUNT(*) FROM fact_air_quality;"
```

---

## 11. FAQ ET DÉPANNAGE

### 11.1 Problèmes Courants

| Problème | Solution |
|----------|----------|
| API Key invalide | Vérifier OPENWEATHER_API_KEY dans .env |
| Airflow ne démarre pas | `docker-compose down -v && docker-compose up -d` |
| raw/ vide | Lancer le backfill: `python scripts/extract_backfill.py` |
| clean/ vide | Lancer transform: `python scripts/transform.py` |
| Doublons dans clean/ | Vérifier la déduplication dans transform.py |

### 11.2 Commandes de Débogage

```bash
# Voir les logs Airflow
docker-compose logs -f webserver

# Vérifier raw/
docker-compose exec minio mc ls local/air-quality-datalake/raw/

# Vérifier clean/
head -n 10 data/clean/air_quality.csv

# Vérifier Data Warehouse
docker-compose exec postgres_warehouse psql -U warehouse -d air_quality_db
```

---

## 12. RAPPORT DE PROJET

### 12.1 Méthode de Travail

| Semaine | Activités |
|---------|-----------|
| **S1** | Mise en place architecture, Docker, Airflow |
| **S2** | Extraction API, stockage raw/ |
| **S3** | Transformation, clean/, Data Warehouse |
| **S4** | Tests, validation, documentation |
| **S5** | Déploiement, vidéo, livraison |

### 12.2 Répartition des Tâches

| Membre | Tâches | Difficultés | Solutions |
|--------|--------|-------------|-----------|
| **Vous** | Architecture, Airflow | Configuration Celery | Passage en LocalExecutor |
| **Nomena** | Extraction API | Rate limiting | Retry avec backoff |
| **Miharintsoa** | Transformation | Déduplication | UPSERT dans clean/ |
| **Lucas** | Data Warehouse | Modélisation | Schéma en étoile simplifié |
| **Membre 5** | Tests, Validation | Validation automatique | Script de validation |

### 12.3 Difficultés Rencontrées

1. **Rate limiting API** : Implémentation de retry avec backoff exponentiel
2. **Déduplication** : Gestion des doublons dans clean/ avec UPSERT
3. **Cohérence des données** : Vérification automatique avec script de validation

---

## 📋 CHECKLIST FINALE

### Infrastructure
- [ ] Docker fonctionne
- [ ] Airflow UI accessible (http://localhost:8080)
- [ ] MinIO accessible (http://localhost:9001)
- [ ] PostgreSQL accessible (localhost:5433)

### Pipeline
- [ ] DAG Airflow activé
- [ ] Exécutions horaires automatiques
- [ ] 5 jours d'historique dans Airflow

### Données
- [ ] raw/ contient 12 mois de données
- [ ] clean/air_quality.csv généré
- [ ] clean/ validé par validate_clean.py

### Data Warehouse
- [ ] Tables créées (dim_city, dim_time, fact_air_quality)
- [ ] Données chargées
- [ ] Cohérence vérifiée

### Documentation
- [ ] ARCHITECTURE.md complet
- [ ] README_STORAGE.md complet
- [ ] README.md complet

### Livrables
- [ ] Dépôt GitHub complet
- [ ] Vidéo 3 minutes
- [ ] Rapport de projet

---

## 🎯 ARRÊT DE LA VIDÉO AU DATA WAREHOUSE

**La vidéo de démonstration (3 min max) doit montrer :**

1. **Pipeline qui tourne** (30s) : Airflow UI montrant plusieurs runs réussis sur 5 jours différents
2. **Zones de stockage** (45s) : 
   - raw/ : fichiers JSON par ville
   - clean/ : fichier CSV unique
3. **Requête SQL sur le Data Warehouse** (45s) : 
   - Compter les lignes
   - AQI moyen par ville
   - Cohérence vérifiée

**Pas de visualisation/dashboard** - s'arrêter au Data Warehouse.

---

**Bonne réalisation ! 🚀**