#!/bin/bash
# test_transform.sh - Test de la transformation des données

echo "🔄 TEST TRANSFORMATION"
echo "====================="
echo ""

# Aller à la racine du projet
cd "$(dirname "$0")/.." || exit

# 1. Vérifier que le conteneur tourne
if ! docker compose -f docker-compose.yml ps 2>/dev/null | grep -q "webserver.*Up"; then
    echo "❌ Le conteneur webserver n'est pas en cours d'exécution"
    echo "👉 Lancez: docker compose -f docker-compose.yml up -d"
    exit 1
fi

# 2. Vérifier qu'il y a des données dans raw/
echo "📂 Vérification des données brutes..."
raw_count=$(docker compose -f docker-compose.yml exec webserver ls -1 /opt/airflow/data/raw/*.json 2>/dev/null | wc -l)
if [ "$raw_count" -eq 0 ]; then
    echo "❌ Aucune donnée brute dans data/raw/"
    echo "👉 Lancez d'abord l'extraction: ./tests/test_extract.sh"
    exit 1
fi
echo "✅ $raw_count fichiers JSON trouvés dans data/raw/"

# 3. Copier le script de transformation
echo "📄 Copie du script de transformation..."
docker cp scripts/transform.py airflow_webserver:/opt/airflow/scripts/transform.py

# 4. Créer le dossier clean/
echo "📁 Création du dossier data/clean/..."
docker compose -f docker-compose.yml exec webserver mkdir -p /opt/airflow/data/clean
docker compose -f docker-compose.yml exec webserver chmod -R 777 /opt/airflow/data/clean

# 5. Exécuter la transformation
echo ""
echo "🚀 Exécution de la transformation..."
docker compose -f docker-compose.yml exec webserver python /opt/airflow/scripts/transform.py

# 6. Vérifier le fichier CSV généré
echo ""
echo "📂 Fichiers créés dans data/clean/ :"
docker compose -f docker-compose.yml exec webserver ls -la /opt/airflow/data/clean/ | grep -E "\.csv$"

echo ""
echo "✅ TEST TRANSFORMATION TERMINÉ"