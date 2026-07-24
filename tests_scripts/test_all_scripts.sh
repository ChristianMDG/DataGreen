#!/bin/bash
# test_all.sh - Test de tout le pipeline

echo "🧪 TEST PIPELINE COMPLET"
echo "========================"
echo ""

# Aller à la racine du projet
cd "$(dirname "$0")/.." || exit

# Vérifier que les conteneurs tournent
if ! docker compose -f docker-compose.yml ps 2>/dev/null | grep -q "webserver.*Up"; then
    echo "❌ Les conteneurs ne sont pas en cours d'exécution"
    echo "👉 Lancez: docker compose -f docker-compose.yml up -d"
    exit 1
fi

# Fonction pour exécuter un test
run_test() {
    echo "═══════════════════════════════════════"
    echo "🔄 $1"
    echo "═══════════════════════════════════════"
    echo ""
    "./tests/$2"
    echo ""
    sleep 2
}

# Exécuter les tests dans l'ordre
run_test "EXTRACTION" "test_extract.sh"
run_test "TRANSFORMATION" "test_transform.sh"
run_test "VALIDATION" "test_validate.sh"
run_test "CHARGEMENT DW" "test_load.sh"

echo "═══════════════════════════════════════"
echo "🎉 PIPELINE COMPLET TERMINÉ AVEC SUCCÈS"
echo "═══════════════════════════════════════"