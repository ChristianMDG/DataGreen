.PHONY: help setup up down restart logs clean test deploy

help:
	@echo "📋 Commandes disponibles :"
	@echo "  make setup    - Configurer l'environnement"
	@echo "  make up       - Démarrer tous les services"
	@echo "  make down     - Arrêter tous les services"
	@echo "  make restart  - Redémarrer tous les services"
	@echo "  make logs     - Afficher les logs"
	@echo "  make clean    - Nettoyer les données"
	@echo "  make test     - Exécuter les tests"
	@echo "  make deploy   - Déployer en production"

setup:
	@echo "📦 Configuration de l'environnement..."
	pip install -r requirements.txt
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@echo "✅ Configuration terminée"
	@echo "⚠️  Éditez le fichier .env pour ajouter votre API key"

up:
	@echo "🚀 Démarrage des services..."
	docker compose -f docker-compose.yml up -d
	@echo "✅ Services démarrés"
	@echo "🌐 Airflow UI: http://localhost:8080 (admin/admin)"

down:
	@echo "🛑 Arrêt des services..."
	docker compose -f docker-compose.yml down
	@echo "✅ Services arrêtés"

restart:
	@echo "🔄 Redémarrage des services..."
	docker compose -f docker-compose.yml down
	docker compose -f docker-compose.yml up -d
	@echo "✅ Services redémarrés"

logs:
	@echo "📋 Affichage des logs..."
	docker compose -f docker-compose.yml logs -f

clean:
	@echo "🧹 Nettoyage..."
	rm -rf data/raw/* data/clean/* logs/*
	@echo "✅ Nettoyage terminé"

test:
	@echo "🧪 Exécution des tests..."
	python -m pytest tests/ -v
	@echo "✅ Tests terminés"

deploy:
	@echo "🚀 Déploiement en production..."
	chmod +x deploy_prod.sh
	./deploy_prod.sh
	@echo "✅ Déploiement terminé"

status:
	@echo "📊 État des services..."
	docker compose -f docker-compose.yml ps

shell:
	@echo "🐚 Connexion au conteneur Airflow..."
	docker compose -f docker-compose.yml exec webserver bash

db:
	@echo "🐘 Connexion à PostgreSQL Warehouse..."
	docker compose -f docker-compose.yml exec postgres_warehouse psql -U warehouse -d air_quality_db