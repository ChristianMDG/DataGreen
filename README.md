# 🌍 Air Quality Pipeline - DataGreen

## Pipeline Automatisé de Collecte et d'Analyse de la Qualité de l'Air

---

[![Docker](https://img.shields.io/badge/Docker-20.10+-blue.svg?style=flat-square&logo=docker)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.7.0-orange.svg?style=flat-square&logo=apache-airflow)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-336791.svg?style=flat-square&logo=postgresql)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3.9-3776AB.svg?style=flat-square&logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)](LICENSE)

---

## 📋 Table des Matières

1. [Présentation du Projet](#-présentation-du-projet)
2. [Architecture Technique](#-architecture-technique)
3. [Technologies Utilisées](#-technologies-utilisées)
4. [Installation](#-installation)
5. [Configuration](#-configuration)
6. [Démarrage](#-démarrage)
7. [Accès aux Services](#-accès-aux-services)
8. [Structure du Projet](#-structure-du-projet)
9. [Guide par Rôle](#-guide-par-rôle)
10. [Commandes Utiles](#-commandes-utiles)
11. [Dépannage](#-dépannage)
12. [Workflow Git](#-workflow-git)
13. [Documentation](#-documentation)
14. [Contributeurs](#-contributeurs)
15. [Licence](#-licence)

---

## 🎯 Présentation du Projet

### Contexte

Ce projet a été développé dans le cadre du cours **Donnée 2**. Il consiste à déployer un pipeline de données automatisé pour collecter, transformer et stocker des données de qualité de l'air.

### Objectif

- Collecter automatiquement les données de qualité de l'air (AQI) pour **5 villes européennes**
- Stocker les données brutes dans une zone **raw/** intouchable
- Générer un fichier **clean/** unique et dédoublonné
- Alimenter un **Data Warehouse** en modèle étoile
- Fournir une infrastructure prête pour l'analyse et la visualisation

### Villes Surveillées

| Ville | Pays | Latitude | Longitude |
|-------|------|----------|-----------|
| **Paris** | 🇫🇷 France | 48.8566 | 2.3522 |
| **London** | 🇬🇧 Royaume-Uni | 51.5074 | -0.1278 |
| **Berlin** | 🇩🇪 Allemagne | 52.5200 | 13.4050 |
| **Madrid** | 🇪🇸 Espagne | 40.4168 | -3.7038 |
| **Rome** | 🇮🇹 Italie | 41.9028 | 12.4964 |

---

## 🏗️ Architecture Technique

### Vue d'Ensemble

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          PIPELINE DE QUALITÉ DE L'AIR                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                     API OpenWeather                                  │  │
│  │              (Qualité de l'air - Données horaires)                   │  │
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

### Flux de Données

```
1. API OpenWeather (Toutes les heures)
   ↓
2. Extraction (PythonOperator Airflow)
   ↓
3. Stockage Brut (raw/ - JSON)
   ↓
4. Transformation (Pandas)
   ↓
5. Stockage Nettoyé (clean/ - CSV)
   ↓
6. Chargement (UPSERT PostgreSQL)
   ↓
7. Data Warehouse (Modèle Étoile)
```

---

## 💻 Technologies Utilisées

| Catégorie | Technologie | Version | Utilisation |
|-----------|-------------|---------|-------------|
| **Orchestration** | Apache Airflow | 2.7.0 | Orchestration du pipeline ETL |
| **Base de données** | PostgreSQL | 14 | Métadonnées Airflow & Data Warehouse |
| **Stockage** | MinIO / AWS S3 | Latest | Data Lake (raw/ + clean/) |
| **Traitement** | Python + Pandas | 3.9 | Extraction, transformation, chargement |
| **Conteneurisation** | Docker + Docker Compose | Latest | Environnement de développement |
| **Versionnement** | Git + GitHub | - | Gestion de code et collaboration |
| **Visualisation** | Apache Superset (optionnel) | 2.1.0 | Dashboard d'analyse |

---

## 📦 Installation

### Prérequis

| Outil | Version Minimum | Vérification |
|-------|----------------|--------------|
| **Docker** | 20.10+ | `docker --version` |
| **Docker Compose** | 1.29+ | `docker compose version` |
| **Git** | 2.30+ | `git --version` |
| **Python** | 3.9+ (optionnel) | `python --version` |

### Installation par Système

#### 🐧 Fedora / RHEL

```bash
# 1. Installer Docker
sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
sudo dnf install docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
newgrp docker

# 2. Vérifier
docker --version
docker compose version

# 3. Installer Git
sudo dnf install git
```

#### 🐧 Ubuntu / Debian

```bash
# 1. Installer Docker
sudo apt update
sudo apt install docker.io docker-compose
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
newgrp docker

# 2. Vérifier
docker --version
docker compose version

# 3. Installer Git
sudo apt install git
```

#### 🍎 macOS

```bash
# 1. Installer Docker Desktop
# Télécharger depuis : https://www.docker.com/products/docker-desktop

# 2. Installer Homebrew (si pas fait)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 3. Installer Git
brew install git

# 4. Vérifier
docker --version
git --version
```

#### 🪟 Windows

```bash
# 1. Installer Docker Desktop
# Télécharger depuis : https://www.docker.com/products/docker-desktop

# 2. Installer Git
# Télécharger depuis : https://git-scm.com/download/win

# 3. Vérifier dans PowerShell
docker --version
git --version
```

### Cloner le Projet

```bash
# Cloner le dépôt
git clone https://github.com/votre-organisation/DataGreen.git
cd DataGreen

# Voir la structure
ls -la
```

---

## ⚙️ Configuration

### Créer le Fichier .env

```bash
# Créer le fichier .env
cp .env.example .env

# OU
touch .env
```

### Contenu du .env

```bash
# ============================================
# API OPENWEATHER - OBLIGATOIRE
# ============================================
# Obtenez votre clé sur : https://openweathermap.org/api
OPENWEATHER_API_KEY=votre_cle_api_ici

# ============================================
# AIRFLOW CONFIGURATION
# ============================================
AIRFLOW_UID=50000
AIRFLOW_GID=50000

# PostgreSQL Airflow (Métadonnées)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# PostgreSQL Data Warehouse
WAREHOUSE_USER=warehouse
WAREHOUSE_PASSWORD=warehouse
WAREHOUSE_DB=air_quality_db

# MinIO (Stockage S3 compatible)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=air-quality-datalake

# Environnement
ENVIRONMENT=dev
FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=
```

### Obtenir une Clé API OpenWeather

1. Aller sur [OpenWeather](https://openweathermap.org/api)
2. Créer un compte (gratuit)
3. Dans le dashboard, aller à **"API Keys"**
4. Copier la clé générée
5. La coller dans `.env` à `OPENWEATHER_API_KEY=`

### ⚠️ Sécurité

```bash
# NE JAMAIS COMMITER LE FICHIER .env
# .gitignore contient déjà :
.env
*.env
```

---

## 🚀 Démarrage

### Étape 1 : Build des Images

```bash
# Build des images Docker
docker compose -f docker-compose.yml build

# En cas de problème
docker compose -f docker-compose.yml build --no-cache
```

### Étape 2 : Démarrer les Services

```bash
# Démarrer tous les services
docker compose -f docker-compose.yml up -d

# Voir les logs (optionnel)
docker compose -f docker-compose.yml logs -f
```

### Étape 3 : Vérifier l'État

```bash
# Voir l'état des conteneurs
docker compose -f docker-compose.yml ps

# Résultat attendu :
# NAME                IMAGE                STATUS
# postgres_airflow    postgres:14          Up (healthy)
# postgres_warehouse  postgres:14          Up (healthy)
# minio               minio/minio:latest   Up
# airflow_webserver   docker-webserver     Up (healthy)
# airflow_scheduler   docker-scheduler     Up
```

### Étape 4 : Initialiser Airflow

```bash
# Attendre que PostgreSQL soit prêt
sleep 15

# Initialiser la base de données
docker compose -f docker-compose.yml exec -T webserver bash << 'EOF'
airflow db init
airflow db migrate
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@airquality.com \
    --password admin
EOF

# Redémarrer les services
docker compose -f docker-compose.yml restart webserver scheduler
sleep 20
```

### Étape 5 : Tester

```bash
# Tester Airflow
curl http://localhost:8080/health
# ✅ Résultat : {"status":"healthy"}

# Ouvrir dans le navigateur
# http://localhost:8080
# Identifiant : admin
# Mot de passe : admin
```

---

## 🌐 Accès aux Services

### Interface Web

| Service | URL | Identifiant | Mot de passe |
|---------|-----|-------------|--------------|
| **Airflow UI** | http://localhost:8080 | admin | admin |
| **MinIO Console** | http://localhost:9001 | minioadmin | minioadmin |
| **PgAdmin** | http://localhost:5050 | admin@airquality.com | admin |

### Connexion aux Bases de Données

```bash
# Data Warehouse (PostgreSQL)
docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db

# Métadonnées Airflow
docker compose -f docker-compose.yml exec postgres_airflow psql -U airflow -d airflow
```

---

## 📂 Structure du Projet

```
DataGreen/
│
├── docker/                          # Configuration Docker
│   ├── Dockerfile.airflow          # Image Airflow personnalisée
│   └── docker-compose.yml          # Services complets
│
├── dags/                            # DAGs Airflow
│   ├── air_quality_pipeline.py     # DAG ETL principal
│   ├── air_quality_backfill.py     # Backfill historique
│   └── air_quality_monitoring.py   # Monitoring
│
├── scripts/                         # Scripts Python
│   ├── extract.py                  # Extraction API (Nomena)
│   ├── transform.py                # Transformation (Miharintsoa)
│   ├── load_warehouse.py           # Chargement DW (Lucas)
│   └── validate_clean.py           # Validation données
│
├── sql/                             # Scripts SQL
│   ├── create_dw.sql               # Schéma Data Warehouse
│   └── analytics_queries.sql       # Requêtes d'analyse
│
├── data/                            # Données
│   ├── raw/                        # Brutes (intouchables)
│   └── clean/                      # Nettoyées (CSV unique)
│
├── tests/                           # Tests unitaires
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
│
├── docs/                            # Documentation
│   ├── ARCHITECTURE.md
│   └── README_STORAGE.md
│
├── .env                             # Variables d'environnement
├── .env.example                     # Exemple de .env
├── .gitignore                       # Fichiers ignorés
├── requirements.txt                 # Dépendances Python
├── Makefile                         # Commandes make
└── README.md                        # Ce fichier
```

---

## 👥 Guide par Rôle

### 🔴 Nomena - Extraction des Données

**Responsabilités :**
- Implémenter `scripts/extract.py`
- Gérer l'appel à l'API OpenWeather
- Sauvegarder les fichiers JSON dans `raw/`
- Gérer les erreurs et les retries

**Commandes pour tester :**

```bash
# Tester l'extraction pour une ville
docker compose -f docker-compose.yml exec webserver python /opt/airflow/scripts/extract.py Paris

# Voir les fichiers créés
docker compose -f docker-compose.yml exec webserver ls -la /opt/airflow/data/raw/air_quality/paris/

# Voir le contenu d'un fichier JSON
docker compose -f docker-compose.yml exec webserver cat /opt/airflow/data/raw/air_quality/paris/*.json | head -50

# Tester les 5 villes
for city in Paris London Berlin Madrid Rome; do
    docker compose -f docker-compose.yml exec webserver python /opt/airflow/scripts/extract.py $city
done

# Déclencher le backfill
docker compose -f docker-compose.yml exec webserver airflow dags trigger air_quality_backfill
```

**Fichiers à modifier :**
- `scripts/extract.py`
- `tests/test_extract.py`

---

### 🟢 Miharintsoa - Transformation

**Responsabilités :**
- Implémenter `scripts/transform.py`
- Lire les fichiers JSON de `raw/`
- Générer `clean/air_quality.csv` unique
- Dédupliquer les données (même ville + même heure)
- Valider la qualité des données

**Commandes pour tester :**

```bash
# Lancer la transformation
docker compose -f docker-compose.yml exec webserver python /opt/airflow/scripts/transform.py

# Voir le CSV généré
docker compose -f docker-compose.yml exec webserver ls -la /opt/airflow/data/clean/

# Lire le contenu
docker compose -f docker-compose.yml exec webserver cat /opt/airflow/data/clean/air_quality_*.csv | head -20

# Valider le fichier
docker compose -f docker-compose.yml exec webserver python /opt/airflow/scripts/validate_clean.py

# Compter les lignes
docker compose -f docker-compose.yml exec webserver wc -l /opt/airflow/data/clean/air_quality_*.csv
```

**Fichiers à modifier :**
- `scripts/transform.py`
- `scripts/validate_clean.py`
- `tests/test_transform.py`

---

### 🟡 Lucas - Data Warehouse

**Responsabilités :**
- Concevoir le modèle en étoile
- Créer `sql/create_dw.sql`
- Implémenter `scripts/load_warehouse.py`
- Charger les données de `clean/` vers PostgreSQL

**Commandes pour tester :**

```bash
# Créer les tables
docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db -f /opt/airflow/sql/create_dw.sql

# Voir les tables
docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db -c "\dt"

# Charger les données
docker compose -f docker-compose.yml exec webserver python /opt/airflow/scripts/load_warehouse.py

# Vérifier le nombre de lignes
docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db -c "SELECT COUNT(*) FROM fact_air_quality;"

# Voir les données
docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db -c "SELECT * FROM fact_air_quality LIMIT 10;"
```

**Fichiers à modifier :**
- `sql/create_dw.sql`
- `scripts/load_warehouse.py`
- `sql/analytics_queries.sql`

---

## 🛠️ Commandes Utiles

### Gestion des Services

```bash
# Démarrer
docker compose -f docker-compose.yml up -d

# Arrêter
docker compose -f docker-compose.yml down

# Redémarrer un service
docker compose -f docker-compose.yml restart webserver

# Voir l'état
docker compose -f docker-compose.yml ps

# Voir les logs
docker compose -f docker-compose.yml logs -f

# Logs d'un service spécifique
docker compose -f docker-compose.yml logs webserver
```

### Gestion d'Airflow

```bash
# Voir tous les DAGs
docker compose -f docker-compose.yml exec webserver airflow dags list

# Voir les erreurs d'import
docker compose -f docker-compose.yml exec webserver airflow dags list-import-errors

# Déclencher un DAG
docker compose -f docker-compose.yml exec webserver airflow dags trigger air_quality_pipeline

# Voir les exécutions
docker compose -f docker-compose.yml exec webserver airflow dags list-runs --dag-id air_quality_pipeline

# Voir les logs d'une tâche
docker compose -f docker-compose.yml exec webserver airflow tasks logs air_quality_pipeline extract_paris 2024-07-16
```

### Gestion des Données

```bash
# Voir raw/
docker compose -f docker-compose.yml exec webserver ls -la /opt/airflow/data/raw/air_quality/

# Voir clean/
docker compose -f docker-compose.yml exec webserver ls -la /opt/airflow/data/clean/

# Voir le Data Warehouse
docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db -c "SELECT * FROM fact_air_quality LIMIT 10;"

# Exporter les données
docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db -c "COPY fact_air_quality TO '/tmp/fact_air_quality.csv' CSV HEADER;"
```

### Nettoyage

```bash
# Nettoyer les données
rm -rf data/raw/* data/clean/*

# Nettoyer Docker
docker system prune -f

# Réinitialisation complète
docker compose -f docker-compose.yml down -v
docker compose -f docker-compose.yml up -d
```

---

## 🔧 Dépannage

### Problème : Airflow ne démarre pas

```bash
# Voir les logs
docker compose -f docker-compose.yml logs webserver

# Réinitialiser la base
docker compose -f docker-compose.yml exec webserver airflow db reset
docker compose -f docker-compose.yml exec webserver airflow db upgrade
```

### Problème : Port déjà utilisé

```bash
# Voir ce qui utilise le port
sudo lsof -i :8080

# Changer le port dans docker-compose.yml
# Modifier "8080:8080" en "8081:8080"
```

### Problème : Permission Docker

```bash
# Ajouter au groupe docker
sudo usermod -aG docker $USER
newgrp docker

# OU utiliser sudo
sudo docker compose -f docker-compose.yml up -d
```

### Problème : Invalid Login Airflow

```bash
# Réinitialiser le mot de passe
docker compose -f docker-compose.yml exec -T webserver airflow users delete --username admin
docker compose -f docker-compose.yml exec -T webserver bash << 'EOF'
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@airquality.com --password admin
EOF
```

### Problème : DAG non visible

```bash
# Voir les erreurs
docker compose -f docker-compose.yml exec webserver airflow dags list-import-errors

# Redémarrer
docker compose -f docker-compose.yml restart webserver scheduler
```

### Problème : API Key invalide

```bash
# Vérifier la clé
cat .env | grep OPENWEATHER_API_KEY

# Tester manuellement
curl "http://api.openweathermap.org/data/2.5/air_pollution?lat=48.8566&lon=2.3522&appid=VOTRE_CLE"
```

---

## 🔄 Workflow Git

### Pour Chaque Membre

```bash
# 1. Récupérer les dernières modifications
git checkout develop
git pull origin develop

# 2. Créer sa branche
git checkout -b feature/ma-fonctionnalite

# 3. Travailler
# ... code ...

# 4. Tester
# ... tests ...

# 5. Commiter
git add .
git commit -m "Feat: Description du changement"

# 6. Pousser
git push origin feature/ma-fonctionnalite

# 7. Créer une Pull Request sur GitHub
```

### Messages de Commit

```bash
# Format recommandé
type(scope): description

# Types
feat: Nouvelle fonctionnalité
fix: Correction de bug
docs: Documentation
style: Formatage
refactor: Refactorisation
test: Tests
chore: Maintenance

# Exemples
feat(extract): Ajout de l'extraction pour Paris
fix(transform): Correction de la déduplication
docs(readme): Mise à jour de la documentation
```

---

## 📚 Documentation

### Documents Disponibles

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Architecture technique détaillée |
| [README_STORAGE.md](docs/README_STORAGE.md) | Documentation du stockage |
| [API_REFERENCE.md](docs/API_REFERENCE.md) | Référence de l'API |

### Liens Utiles

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [OpenWeather API](https://openweathermap.org/api/air-pollution)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Documentation](https://docs.docker.com/)

---

## 👥 Contributeurs

| Membre | Rôle | Responsabilités |
|--------|------|-----------------|
| **[Votre Nom]** | Lead / Architecte | Architecture, Airflow, CI/CD |
| **Nomena** | Data Engineer | Extraction API, raw/ |
| **Miharintsoa** | Data Engineer | Transformation, clean/ |
| **Lucas** | Data Analyst | Data Warehouse, SQL |

---

## 📄 Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.

---

## 🙏 Remerciements

- OpenWeather pour leur API
- Apache Airflow pour l'orchestration
- Docker pour la conteneurisation

---

**Bon développement à tous ! 🚀**

---

## 📝 Checklist d'Installation

### Pour chaque membre

- [ ] Git installé
- [ ] Docker installé
- [ ] Docker Compose installé
- [ ] Projet cloné
- [ ] .env configuré
- [ ] Clé API OpenWeather obtenue
- [ ] Services démarrés (`docker compose up -d`)
- [ ] Airflow accessible (http://localhost:8080)
- [ ] admin/admin fonctionne
- [ ] DAGs visibles dans Airflow

---

**Dernière mise à jour :** 17 Juillet 2024