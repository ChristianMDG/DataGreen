#!/bin/bash
# test_validate.sh - Test de la validation des données

echo "✅ TEST VALIDATION"
echo "================="
echo ""

# Aller à la racine du projet
cd "$(dirname "$0")/.." || exit

# 1. Vérifier que le conteneur tourne
if ! docker compose -f docker-compose.yml ps 2>/dev/null | grep -q "webserver.*Up"; then
    echo "❌ Le conteneur webserver n'est pas en cours d'exécution"
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

# 3. Copier le script de validation
echo "📄 Copie du script de validation..."
docker cp scripts/validate_clean.py airflow_webserver:/opt/airflow/scripts/validate_clean.py

# 4. Exécuter la validation
echo ""
echo "🚀 Exécution de la validation..."
docker compose -f docker-compose.yml exec webserver python /opt/airflow/scripts/validate_clean.py

# 5. Vérifier le résultat
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Validation clean/ : SUCCÈS"
else
    echo ""
    echo "❌ Validation clean/ : ÉCHEC"
    exit 1
fi

echo ""
echo "✅ TEST VALIDATION TERMINÉ"