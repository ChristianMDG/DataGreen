
---

# Projet IAI – Analyse et Automatisation des Données de Qualité de l’Air

## Contexte

Ce projet s’inscrit dans la continuité des travaux réalisés en groupe lors de l’examen.  
Les données manipulées dans ce cadre seront réutilisées pour un projet individuel, avec un accent particulier sur :

- L’analyse exploratoire  
- La visualisation  
- La mise en production d’un pipeline de données  

---

## Objectif général

Déployer un pipeline de données **fonctionnel en production**, et pas seulement en exécution locale.  
Ce pipeline devra ingérer, stocker, transformer et modéliser des données de qualité de l’air, issues d’une API publique, afin de permettre une analyse historique.

---

## Source de données

- **API utilisée** : Open Weather Data – Air Pollution API  
- **Villes concernées** : minimum 5 villes différentes  

---

## Architecture cible

L’architecture devra inclure les éléments suivants :

| Composant       | Technologie / Service suggéré                          |
|-----------------|--------------------------------------------------------|
| **Orchestration** | Apache Airflow (Oracle Cloud, AWS, etc.) **ou** Serverless (AWS Lambda / GCP Cloud Functions) |
| **Déclenchement** | EventBridge (AWS) ou équivalent                        |
| **Extraction**  | Appel périodique à l’API (ex. toutes les heures)      |
| **Data Lake**   | AWS S3 **ou** GCP Storage                             |
| **Transformation** | Nettoyage et consolidation dans un dossier `clean/` contenant l’historique complet |
| **Modélisation** | Schéma en **étoile** ou **en flocon** (Data Warehouse) |
| **Historique**  | Idéalement 12 mois de données, **minimum 4 mois**     |

---

## Livrables attendus

L’ensemble des travaux devra être versionné sur **GitHub** avec :

- Une **codebase** complète et exécutable  
- Une **architecture documentée** (choix technologiques, modélisation, flux de données)  
- Un fichier **README** clair et structuré  

---

## Modalités de rendu

- **Dépôt GitHub** : ajouter votre collaborateur en tant que *reviewer* sur une *Merge Request* (MR)  
- **Soutenance finale** : vidéo de **3 minutes** maximum présentant une démonstration fonctionnelle du pipeline en production  

---

## Planning indicatif

- **Durée** : 3 semaines  
- **Jalons recommandés** :
  - Semaine 1 : Choix de l’architecture + ingestion  
  - Semaine 2 : Transformation + modélisation  
  - Semaine 3 : Finalisation, documentation et enregistrement vidéo  

---

## Critères d’évaluation (non exhaustifs)

- Pertinence des choix technologiques  
- Qualité du code et structuration du projet  
- Automatisation et résilience du pipeline  
- Profondeur de l’historique de données  
- Clarté de la documentation et de la démonstration  

---

Bonne réalisation !