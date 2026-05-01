.PHONY: help install dev up down logs test test-cov lint format typecheck check clean

UV ?= uv

help:
	@awk 'BEGIN {FS = ":.*##"; printf "Aeroza make targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

install: ## Sync Python dependencies via uv
	$(UV) sync --all-extras

dev: ## Run the API with hot reload
	$(UV) run uvicorn aeroza.main:app --reload --host 0.0.0.0 --port 8000

up: ## Start dev infrastructure (Postgres, Redis, NATS)
	docker compose up -d
	@echo "Postgres :5432  Redis :6379  NATS :4222 (monitoring :8222)"

down: ## Stop dev infrastructure
	docker compose down

logs: ## Tail dev infrastructure logs
	docker compose logs -f

test: ## Run unit tests
	$(UV) run pytest

test-cov: ## Run tests with coverage report
	$(UV) run pytest --cov=aeroza --cov-report=term-missing --cov-report=html

lint: ## Run ruff lint
	$(UV) run ruff check .

format: ## Run ruff formatter
	$(UV) run ruff format .

typecheck: ## Run mypy
	$(UV) run mypy aeroza

check: lint typecheck test ## Run lint, type-check, and tests

clean: ## Remove caches and build artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov dist build *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
