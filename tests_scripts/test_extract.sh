#!/bin/bash
# test_extract.sh - Test de l'extraction des données

echo "📥 TEST EXTRACTION"
echo "=================="
echo ""

# Aller à la racine du projet
cd "$(dirname "$0")/.." || exit

# 1. Vérifier que le script existe
if [ ! -f "scripts/extract.py" ]; then
    echo "❌ Fichier scripts/extract.py manquant"
    exit 1
fi

# 2. Vérifier que le conteneur tourne
if ! docker compose -f docker-compose.yml ps 2>/dev/null | grep -q "webserver.*Up"; then
    echo "❌ Le conteneur webserver n'est pas en cours d'exécution"
    echo "👉 Lancez: docker compose -f docker-compose.yml up -d"
    exit 1
fi

# 3. Vérifier que la clé API est définie
echo "🔑 Vérification de la clé API..."
docker compose -f docker-compose.yml exec webserver env | grep OPENWEATHER_API_KEY || {
    echo "❌ OPENWEATHER_API_KEY non définie"
    echo "👉 Ajoutez-la dans .env ou docker-compose.yml"
    exit 1
}

# 4. Vérifier le dossier data/raw/
echo "📁 Vérification du dossier data/raw/..."
docker compose -f docker-compose.yml exec webserver mkdir -p /opt/airflow/data/raw
docker compose -f docker-compose.yml exec webserver chmod -R 777 /opt/airflow/data/raw

# 5. Copier le script d'extraction
echo "📄 Copie du script d'extraction..."
docker cp scripts/extract.py airflow_webserver:/opt/airflow/scripts/extract.py

# 6. Exécuter l'extraction
echo ""
echo "🚀 Exécution de l'extraction..."
docker compose -f docker-compose.yml exec webserver python /opt/airflow/scripts/extract.py

# 7. Vérifier les fichiers créés
echo ""
echo "📂 Fichiers créés dans data/raw/ :"
docker compose -f docker-compose.yml exec webserver ls -la /opt/airflow/data/raw/ | grep -E "\.json$"

echo ""
echo "✅ TEST EXTRACTION TERMINÉ"