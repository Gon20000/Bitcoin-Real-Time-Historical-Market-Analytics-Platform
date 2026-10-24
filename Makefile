.PHONY: help init build up-full up-stream up-orch up-analytics down logs clean \
        spark-batch dbt-shell dbt-run dbt-test test lint

# Default target
help:
	@echo "BTC/USDT 1-Second Analytics Platform - CLI Shortcuts"
	@echo "----------------------------------------------------"
	@echo "Setup & Lifecycle:"
	@echo "  make init          - Create .env, local data/ lake folders, and setup pre-commit"
	@echo "  make build         - Build custom Airflow, dbt, and Kafka Producer images"
	@echo "  make up-full       - Start the entire platform (all layers)"
	@echo "  make up-stream     - Start Speed Layer only (Kafka + WS Producer + Spark Streaming)"
	@echo "  make up-orch       - Start Orchestration Layer (Postgres + Airflow + Spark + Trino)"
	@echo "  make up-analytics  - Start Analytics Layer (Trino + dbt Workspace)"
	@echo "  make down          - Stop all running containers"
	@echo "  make clean         - Stop containers and wipe volumes/checkpoints"
	@echo ""
	@echo "Development & Execution:"
	@echo "  make spark-batch   - Trigger historical CSV batch ingestion job on-demand"
	@echo "  make dbt-shell     - Open interactive bash shell inside dbt-workspace"
	@echo "  make dbt-run       - Execute dbt models against Trino"
	@echo "  make dbt-test      - Execute dbt data quality tests"
	@echo "  make test          - Run local pytest unit tests"
	@echo "  make lint          - Run pre-commit formatting checks across all files"

# 1. Project Initialization
init:
	@if [ ! -f .env ]; then cp .env.example .env && echo "Created .env from .env.example"; fi
	mkdir -p data/bronze data/silver data/gold data/checkpoints
	mkdir -p airflow/logs trino/etc/catalog
	@echo "Local Medallion data lake directories initialized."
	pip install -r requirements-dev.txt
	pre-commit install
	@echo "Dev dependencies installed and pre-commit hooks activated."

# 2. Docker Builds & Profile Management
build:
	docker compose --profile full-stack build

up-full:
	docker compose --profile full-stack up -d

up-stream:
	docker compose --profile streaming up -d

up-orch:
	docker compose --profile orchestration up -d

up-analytics:
	docker compose --profile analytics up -d

down:
	docker compose --profile full-stack --profile batch down

logs:
	docker compose logs -f --tail=100

clean:
	docker compose --profile full-stack --profile batch down -v
	rm -rf data/checkpoints/*

# 3. Spark & dbt Operations
spark-batch:
	docker compose --profile batch run --rm spark-batch-job

dbt-shell:
	docker exec -it btc-dbt-workspace bash

dbt-run:
	docker exec -it btc-dbt-workspace dbt run

dbt-test:
	docker exec -it btc-dbt-workspace dbt test

# 4. Testing & Governance
test:
	pytest tests/ -v

lint:
	pre-commit run --all-files
