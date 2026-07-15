.PHONY: help setup up down restart logs clean test

help:
	@echo "📋 Commandes disponibles :"
	@echo "  make setup    - Configurer l'environnement"
	@echo "  make up       - Démarrer tous les services"
	@echo "  make down     - Arrêter tous les services"
	@echo "  make restart  - Redémarrer tous les services"
	@echo "  make logs     - Afficher les logs"
	@echo "  make clean    - Nettoyer les données"
	@echo "  make test     - Exécuter les tests"

setup:
	@echo "📦 Configuration de l'environnement..."
	pip install -r requirements.txt
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@echo "✅ Configuration terminée"
	@echo "⚠️  Éditez le fichier .env pour ajouter votre API key"

up:
	@echo "🚀 Démarrage des services..."
	docker-compose -f docker/docker-compose.yml up -d
	@echo "✅ Services démarrés"
	@echo "🌐 Airflow UI: http://localhost:8080 (admin/admin)"

down:
	@echo "🛑 Arrêt des services..."
	docker-compose -f docker/docker-compose.yml down
	@echo "✅ Services arrêtés"

restart:
	make down && make up

logs:
	docker-compose -f docker/docker-compose.yml logs -f

clean:
	@echo "🧹 Nettoyage..."
	rm -rf data/raw/* data/clean/* logs/*
	@echo "✅ Nettoyage terminé"

test:
	@echo "🧪 Exécution des tests..."
	python -m pytest tests/ -v
	@echo "✅ Tests terminés"
