#!/bin/bash
# test_load.sh - Test du chargement dans le Data Warehouse

echo "📤 TEST CHARGEMENT DW"
echo "===================="
echo ""

# Aller à la racine du projet
cd "$(dirname "$0")/.." || exit

# 1. Vérifier que les conteneurs tournent
if ! docker compose -f docker-compose.yml ps 2>/dev/null | grep -q "webserver.*Up"; then
    echo "❌ Le conteneur webserver n'est pas en cours d'exécution"
    echo "👉 Lancez: docker compose -f docker-compose.yml up -d"
    exit 1
fi

if ! docker compose -f docker-compose.yml ps 2>/dev/null | grep -q "postgres_warehouse.*Up"; then
    echo "❌ Le conteneur postgres_warehouse n'est pas en cours d'exécution"
    echo "👉 Lancez: docker compose -f docker-compose.yml up -d"
    exit 1
fi

# 2. Vérifier qu'il y a des données dans clean/
echo "📂 Vérification des données clean/..."
clean_count=$(docker compose -f docker-compose.yml exec webserver ls -1 /opt/airflow/data/clean/*.csv 2>/dev/null | wc -l)
if [ "$clean_count" -eq 0 ]; then
    echo "❌ Aucune donnée clean/ trouvée"
    echo "👉 Lancez d'abord la transformation: ./tests/test_transform.sh"
    exit 1
fi
echo "✅ $clean_count fichiers CSV trouvés dans data/clean/"

# 3. Copier le script de chargement
echo "📄 Copie du script de chargement..."
docker cp scripts/load_warehouse.py airflow_webserver:/opt/airflow/scripts/load_warehouse.py

# 4. Vérifier les tables du Data Warehouse
echo ""
echo "🏗️ Vérification des tables DW..."
docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db -c "\dt" || {
    echo "⚠️ Tables non trouvées, création..."
    docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db -f sql/create_dw.sql
}

# 5. Exécuter le chargement
echo ""
echo "🚀 Exécution du chargement..."
docker compose -f docker-compose.yml exec webserver python /opt/airflow/scripts/load_warehouse.py

echo ""
echo "✅ TEST CHARGEMENT DW TERMINÉ"