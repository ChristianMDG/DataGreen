
---

# 👥 Répartition des Tâches - Projet Air Quality Pipeline

## 📋 Équipe Projet

| Membre | Rôle Principal | Compétences Clés |
|--------|---------------|------------------|
| **Christian** | Lead / Architecte Data | Architecture, Orchestration, Intégration |
| **Nomena** | Data Engineer | Extraction, APIs, Stockage |
| **Miarintsoa** | Data Engineer | Transformation, Data Quality |
| **Gael** | Data Analyst / BI | Data Warehouse, Visualisation, Dashboard |

---

## 👤 Christian

### Rôle : Coordination, Architecture et Intégration Globale

```yaml
Responsabilités:
  - Architecture globale du système
  - Configuration d'Airflow en production
  - Intégration de tous les composants
  - CI/CD et déploiement
  - Coordination d'équipe
  - Documentation finale
```

### Tâches Détaillées

#### Tache 1 : Infrastructure & Configuration

```markdown
##  1: Setup Environnement
- [ ] Configuration Docker Compose complet
  - Airflow Webserver, Scheduler, Worker
  - PostgreSQL (métadonnées Airflow)
  - PostgreSQL (Data Warehouse)
  - MinIO (S3 local)
  - Redis (Celery broker)
  - Apache Superset
  - PgAdmin

- [ ] Dockerfile personnalisé pour Airflow
  - Ajout des dépendances Python
  - Variables d'environnement
  - Scripts d'entrypoint

- [ ] Configuration Airflow
  - airflow.cfg personnalisé
  - Connexions (API, DB, S3)
  - Variables (cities, API key, paths)

- [ ] Structure du projet sur GitHub
  - Branches: main, develop, feature/*
  - Protection des branches
  - Issues et Project Board

**Livrable**: docker-compose.yml + Dockerfile
```

#### Tache 2 : Orchestration & Intégration

```markdown
## J6-J10 : DAG Airflow et Intégration

- [ ] Création du DAG principal
  - Définition des tâches
  - Dépendances entre tâches
  - Paramètres de schedule
  - Gestion des erreurs (retries, alerts)

- [ ] Task Groups pour organisation
  - Groupe Extraction (par ville)
  - Groupe Transformation
  - Groupe Chargement

- [ ] Sensors et Hooks
  - HttpSensor pour vérifier API
  - PostgresHook pour warehouse
  - S3Hook pour MinIO/S3

- [ ] Variables Airflow
  - cities: ["Paris","London","Berlin","Madrid","Rome"]
  - openweather_api_key: (à configurer)
  - raw_data_path: "/opt/airflow/data/raw"
  - clean_data_path: "/opt/airflow/data/clean"

- [ ] Tests du DAG
  - Validation de la structure
  - Tests unitaires avec pytest
  - Simulation d'exécution

**Livrable**: dags/air_quality_pipeline.py complet
```

#### Tache 3 : Finalisation & Livraison

```markdown
## J13-J18 : Finalisation

- [ ] CI/CD avec GitHub Actions
  - Tests automatiques
  - Build des images Docker
  - Déploiement sur serveur

- [ ] Monitoring et Alerting
  - Logs centralisés
  - Notifications (Email, Slack)
  - Dashboard de monitoring

- [ ] Documentation
  - Architecture globale
  - Guide d'installation
  - Guide de déploiement

- [ ] Coordination des livrables
  - Revue de code de l'équipe
  - Merge Request finale
  - Vidéo de démonstration (introduction/conclusion)

**Livrables**: 
  - README.md complet
  - Vidéo (parties intro/conclusion)
  - GitHub avec MR
```

---

## 👤 NOMENA - Data Engineer

### Rôle : Extraction, APIs et Stockage

```yaml
Responsabilités:
  - Intégration de l'API OpenWeather
  - Scripts d'extraction
  - Gestion du Data Lake (S3/MinIO)
  - Gestion des erreurs d'API
  - Gestion des données historiques
```

### Tâches Détaillées

#### Tache 1 : Préparation des APIs

```markdown
## * Setup API et Stockage

- [ ] Création du compte OpenWeather
  - Récupération de l'API key
  - Test de l'API avec Postman/curl

- [ ] Script d'extraction basique
  - Appel API pour 1 ville
  - Sauvegarde en JSON
  - Structure des données

- [ ] Configuration MinIO (S3 local)
  - Buckets: raw, clean, scripts
  - Politiques d'accès
  - Interface MinIO

- [ ] Mapping des coordonnées des villes
  - Paris: lat=48.8566, lon=2.3522
  - London: lat=51.5074, lon=-0.1278
  - Berlin: lat=52.5200, lon=13.4050
  - Madrid: lat=40.4168, lon=-3.7038
  - Rome: lat=41.9028, lon=12.4964

**Livrable**: scripts/extract.py (version basique)
```

#### Tache 2 : Extraction Avancée

```markdown
## * Extraction Complète

- [ ] Extraction pour 5 villes en parallèle
  - Utilisation de ThreadPoolExecutor
  - Gestion des timeouts
  - Logging détaillé

- [ ] Gestion des erreurs robuste
  - Retry sur échec
  - Exponential backoff
  - Alertes en cas d'échec

- [ ] Extraction historique
  - Récupération des 4 derniers mois
  - API historique (ou alternative)
  - Stockage partitionné par date

- [ ] Integration avec Airflow
  - PythonOperator pour extraction
  - Task Group par ville
  - Paramètres de retry

- [ ] Stockage dans S3/MinIO
  - Upload des fichiers JSON
  - Organisation: raw/air_quality/{city}/{date}/
  - Vérification des uploads

**Livrable**: scripts/extract.py (version complète)
```

#### Tache 3 : Optimisation et Documentation

```markdown
## *  Finalisation Extraction

- [ ] Optimisation des performances
  - Cache des requêtes
  - Batch processing
  - Compression des données

- [ ] Documentation de l'API
  - Swagger/OpenAPI
  - Exemples d'utilisation
  - Guide pour ajouter des villes

- [ ] Tests de l'extracteur
  - Tests unitaires
  - Tests d'intégration
  - Mock de l'API

- [ ] Contribution à la documentation finale
  - Section Extraction dans README
  - Diagrammes de flux
  - Guide de troubleshooting

**Livrables**:
  - scripts/extract.py final
  - tests/test_extract.py
  - Documentation extraction
  - Vidéo (section extraction, 30s)
```

---

## 👤 MIHARINTSOA - Data Engineer

### Rôle : Transformation et Qualité des Données

```yaml
Responsabilités:
  - Nettoyage des données brutes
  - Enrichissement des données
  - Contrôle de qualité des données
  - Gestion des valeurs aberrantes
  - Intégration des tests de qualité
```

### Tâches Détaillées

#### Tache 1 : Analyse et Préparation

```markdown
## * : Analyse des Données

- [ ] Analyse exploratoire des données
  - Structure des fichiers JSON
  - Schéma des données
  - Statistiques descriptives

- [ ] Définition des règles de nettoyage
  - Gestion des valeurs manquantes
  - Détection des outliers
  - Standardisation des formats

- [ ] Script de transformation basique
  - Lecture des JSON
  - Conversion en DataFrame
  - Sauvegarde en CSV

- [ ] Configuration du dossier clean
  - Structure: clean/air_quality/
  - Format: Parquet (optimisé)

**Livrable**: scripts/transform.py (version basique)
```

#### Tache 2 : Transformation Avancée

```markdown
## * : Pipeline de Transformation

- [ ] Transformation complète
  - Ajout des dimensions temporelles
  - Date, heure, jour de semain
  - Mois, année, saison
  - Weekend ou non

- [ ] Enrichissement des données
  - Catégorie AQI
  - Ratio PM2.5/PM10
  - Indicateurs de qualité

- [ ] Validation de la qualité
  - Great Expectations
  - Profiling des données
  - Rapport de qualité

- [ ] Intégration avec Airflow
  - PythonOperator pour transform
  - Gestion des erreurs
  - Logging détaillé

**Livrable**: scripts/transform.py (version complète)
```

#### Tache 3 : Optimisation et Documentation

```markdown
## * : Finalisation Transformation

- [ ] Optimisation des performances
  - Vectorization Pandas
  - Parquet vs CSV
  - Partitionnement par date

- [ ] Vérifications de qualité avancées
  - Tests de cohérence temporelle
  - Détection de dérive des capteurs
  - Alertes de qualité

- [ ] Documentation du pipeline de transformation
  - Règles de nettoyage
  - Features créées
  - Guide d'utilisation

**Livrables**:
  - scripts/transform.py final
  - scripts/quality_checks.py
  - tests/test_quality.py
  - Documentation transformation
  - Vidéo (section transformation, 30s)
```

---

## 👤 Gael - Data Analyst / BI

### Rôle : Data Warehouse, Visualisation et Dashboard

```yaml
Responsabilités:
  - Modélisation des données (Star Schema)
  - Configuration du Data Warehouse
  - Création des dashboards
  - Requêtes d'analyse
  - Visualisations (Superset/Tableau/Looker Studio)
```

### Tâches Détaillées

#### Tâche 1 : Modélisation et Data Warehouse

```markdown
## * : Conception du Data Warehouse

- [ ] Conception du modèle en étoile
  - Dimension Ville (dim_city)
  - Dimension Temps (dim_time)
  - Table Fait (fact_air_quality)

- [ ] Scripts de création des tables
  - SQL pour PostgreSQL
  - Index et contraintes
  - Optimisation des requêtes

- [ ] Configuration du Data Warehouse
  - PostgreSQL (docker-compose)
  - Connexions et credentials
  - Migration automatique

- [ ] Scripts de chargement initial
  - Import des données clean
  - UPSERT pour éviter doublons

**Livrable**: sql/create_warehouse_tables.sql
```

#### Tâche 2 : Chargement et Dashboard

```markdown
## : Chargement des Données et Dashboard

- [ ] Script de chargement (load.py)
  - UPSERT dans les dimensions
  - Insertion des faits
  - Gestion des doublons
  - Batch processing

- [ ] Requêtes d'analyse
  - AQI moyen par ville
  - Évolution temporelle
  - Comparaison des polluants
  - Alertes et anomalies

- [ ] Configuration Superset
  - Connexion à PostgreSQL
  - Datasets
  - Charts

- [ ] Création du Dashboard
  - KPIs principaux
  - Graphiques temporels
  - Comparaisons ville à ville
  - Alertes

**Livrable**: scripts/load.py + Dashboard Superset
```

#### Tâche 3 : Finalisation et Documentation

```markdown
## * : Finalisation Dashboard

- [ ] Dashboard complet
  - Filtres interactifs (ville, date)
  - Graphiques optimisés
  - Alertes temps réel
  - Export PDF/CSV

- [ ] Requêtes d'analyse avancées
  - Analyse des tendances
  - Forecast simple
  - Comparaisons multiples

- [ ] Documentation finale
  - Guide d'utilisation Superset
  - Requêtes utiles
  - Interprétation des données

- [ ] Vidéo de démonstration
  - Présentation du dashboard
  - Exemples d'analyse

**Livrables**:
  - Dashboard final (Superset)
  - sql/analytics_queries.sql
  - Documentation du Data Warehouse
  - Vidéo (section dashboard, 45s)
```

---

## 📊 Dashboard à Créer par Lucas

### Structure du Dashboard Superset

```yaml
Dashboard Name: "Qualité de l'Air - Monitoring"

Pages:
  1. Vue d'ensemble:
    - KPIs: AQI Moyen, Ville la plus polluée, Nb de mesures
    - Graphique: AQI par ville (barres)
    - Graphique: Évolution AQI (ligne)
    - Carte: Heatmap des villes

  2. Analyse par Polluant:
    - Graphique: PM2.5 vs PM10
    - Graphique: Composition des polluants
    - Tableau: Top 10 mesures critiques

  3. Analyse Temporelle:
    - Graphique: AQI par heure (24h)
    - Graphique: AQI par mois (12 mois)
    - Graphique: Comparaison saisonnière

  4. Alertes:
    - Tableau: Dernières alertes
    - Graphique: Fréquence des alertes
    - Carte: Alerte par ville

Filtres:
  - Ville (multiselect)
  - Période (date range)
  - AQI Category (checkbox)
```

### Exemple de Dashboard Looker Studio (Alternative)

```markdown
# Dashboard Looker Studio

## Configuré par Gael avec :

### Sources de données
- PostgreSQL (Data Warehouse)
- Table: fact_air_quality + dimensions

### Visualisations
1. Scorecard: AQI Moyen, Villes, Période
2. Bar Chart: AQI par ville
3. Line Chart: Évolution horaire
4. Table: Détails des mesures
5. Scatter Plot: PM2.5 vs PM10
6. Pie Chart: Répartition AQI Category

### Interactivité
- Filtrer par ville
- Sélection de date
- Drill-down par heure

### Mise à jour
- Automatique (refresh toutes les heures)
- Alerte par email (AQI > 150)
```

---


### Outils de Collaboration

```yaml
GitHub:
  - Repo: air-quality-pipeline
  - Branches: main, develop, feature/*
  - Issues: Tâches et bugs
  - Projects: Kanban

Google Drive:
  - Dossier: Air Quality Project
  - Sous-dossiers: Docs, Présentations, Vidéos

Tools:
  - Docker: Environnement partagé
  - Airflow: Orchestration
  - Superset/Looker: Dashboard
```

---

## 📝 Récapitulatif des Livrables par Membre

### Vous (Lead)
- ✅ docker-compose.yml complet
- ✅ DAG Airflow principal
- ✅ CI/CD (GitHub Actions)
- ✅ README.md final
- ✅ Vidéo (intro + conclusion)

### Nomena (Extraction)
- ✅ scripts/extract.py
- ✅ scripts/extract_historical.py
- ✅ tests/test_extract.py
- ✅ Documentation extraction
- ✅ Vidéo (section extraction)

### Miharintsoa (Transformation)
- ✅ scripts/transform.py
- ✅ scripts/quality_checks.py
- ✅ tests/test_quality.py
- ✅ Documentation transformation
- ✅ Vidéo (section transformation)

### Lucas (BI/Visualisation)
- ✅ sql/create_tables.sql
- ✅ scripts/load.py
- ✅ Dashboard Superset
- ✅ sql/analytics_queries.sql
- ✅ Vidéo (section dashboard)


## ✅ Checklist de Validation Finale

```markdown
# Validation Finale - Chaque membre doit valider

## ✅ Vous
- [ ] Tous les conteneurs Docker fonctionnent
- [ ] Airflow UI accessible
- [ ] DAG s'exécute sans erreur
- [ ] Pipeline complet fonctionnel
- [ ] Documentation finale prête
- [ ] Video finale montée (3 min)

## ✅ Nomena
- [ ] Extraction des 5 villes fonctionne
- [ ] Données stockées dans S3/MinIO
- [ ] Gestion des erreurs robuste
- [ ] Tests d'extraction passent
- [ ] Documentation extraction à jour

## ✅ Miharintsoa
- [ ] Transformation fonctionne sans erreur
- [ ] Qualité des données vérifiée
- [ ] Features dérivées ajoutées
- [ ] Tests de qualité passent
- [ ] Documentation transformation à jour

## ✅ Lucas
- [ ] Data Warehouse créé
- [ ] Modèle en étoile implémenté
- [ ] Dashboard fonctionnel
- [ ] Requêtes d'analyse testées
- [ ] Documentation BI à jour
```

---

## 🔗 Lien vers les Ressources

```markdown
# Ressources Utiles

## Documentation
- OpenWeather API: https://openweathermap.org/api/air-pollution
- Apache Airflow: https://airflow.apache.org/docs/
- PostgreSQL: https://www.postgresql.org/docs/
- Superset: https://superset.apache.org/docs/

## Code
- GitHub Repo: https://github.com/yourteam/air-quality-pipeline
- Docker Hub: https://hub.docker.com/ (images)

```

---

Bonne chance à toute l'équipe ! 🚀