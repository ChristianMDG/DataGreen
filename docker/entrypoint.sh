#!/bin/bash
set -e

# Attendre PostgreSQL
echo "⏳ Attente de PostgreSQL..."
while ! nc -z postgres_airflow 5432; do
  sleep 1
done
echo "✅ PostgreSQL prêt!"

# Initialiser Airflow si nécessaire
if [ ! -f /opt/airflow/airflow.db ]; then
    echo "📦 Initialisation d'Airflow..."
    airflow db init
    airflow db upgrade
    
    # Créer l'utilisateur admin
    echo "👤 Création de l'utilisateur admin..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@airquality.com \
        --password admin
fi

# Exécuter la commande
exec airflow "$@"
