# KPI Intelligence Backend - Docker Management Scripts
# Production-level automation for common docker-compose operations

# Build and run services
docker-build:
	docker-compose build --no-cache

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-restart:
	docker-compose restart

# Production deployment
production-deploy:
	@echo "Deploying production environment..."
	docker-compose down
	docker-compose build --no-cache
	docker-compose up -d
	@echo "Production deployment complete!"

# Database operations
db-init:
	docker-compose exec db psql -U kpiuser -d kpi_intelligence -f /docker-entrypoint-initdb.d/01-schema.sql

db-shell:
	docker-compose exec db psql -U kpiuser -d kpi_intelligence

db-backup:
	docker-compose exec db pg_dump -U kpiuser kpi_intelligence > backup_$(shell date +%Y%m%d_%H%M%S).sql

# Testing
test-local:
	python run_integration_tests.py

test-docker:
	docker-compose --profile test up test

test-docker-build:
	docker-compose --profile test build test

test-all:
	@echo "Running all integration tests..."
	python run_integration_tests.py

# Clean up
clean:
	docker-compose down -v
	rm -rf __pycache__
	rm -rf data/processed/*
	rm -rf logs/*
	rm -rf test_reports/*

clean-all: clean
	docker system prune -af
	docker volume prune -f

# Health checks
health-check:
	@echo "Checking API health..."
	@curl -f http://localhost:8000/health || echo "API is not responding"
	@echo "\nChecking database connection..."
	@docker-compose exec db pg_isready -U kpiuser -d kpi_intelligence

# Logs and monitoring
logs-api:
	docker-compose logs -f api

logs-db:
	docker-compose logs -f db

logs-all:
	docker-compose logs -f

# Development helpers
dev-shell:
	docker-compose exec api /bin/bash

dev-install:
	pip install -r requirements.txt

dev-run:
	python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# Default target
.PHONY: help
help:
	@echo "KPI Intelligence Backend - Available Commands:"
	@echo ""
	@echo "  Production:"
	@echo "    make production-deploy  - Build and deploy production environment"
	@echo "    make docker-build       - Build Docker images"
	@echo "    make docker-up          - Start all services"
	@echo "    make docker-down        - Stop all services"
	@echo ""
	@echo "  Testing:"
	@echo "    make test-local         - Run integration tests locally"
	@echo "    make test-docker        - Run integration tests in Docker"
	@echo "    make test-all           - Run all tests"
	@echo ""
	@echo "  Database:"
	@echo "    make db-init            - Initialize database schema"
	@echo "    make db-shell           - Open database shell"
	@echo "    make db-backup          - Backup database"
	@echo ""
	@echo "  Monitoring:"
	@echo "    make health-check       - Check service health"
	@echo "    make logs-api           - View API logs"
	@echo "    make logs-db            - View database logs"
	@echo ""
	@echo "  Cleanup:"
	@echo "    make clean              - Clean up containers and temp files"
	@echo "    make clean-all          - Deep clean (removes all Docker data)"
