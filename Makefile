.PHONY: help install dev up down logs test test-unit test-integration test-cov \
        lint format typecheck check migrate migrate-down migration db-shell clean \
        web-install web-dev web-build web-typecheck

UV ?= uv

# DSN used by the integration test suite. Override per-environment as needed
# (e.g. `make test-integration TEST_DATABASE_URL=postgresql+asyncpg://...`).
TEST_DATABASE_URL ?= postgresql+asyncpg://aeroza:aeroza@localhost:5432/aeroza_test

help:
	@awk 'BEGIN {FS = ":.*##"; printf "Aeroza make targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

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

test: ## Run unit tests (skips DB-dependent tests automatically)
	$(UV) run pytest -m "not integration"

test-unit: ## Same as `test` — explicit alias
	$(UV) run pytest -m unit

test-integration: ## Run integration tests (requires `make up` to be running)
	@docker compose exec -T postgres psql -U aeroza -d postgres \
		-c "CREATE DATABASE aeroza_test" 2>/dev/null || true
	AEROZA_TEST_DATABASE_URL="$(TEST_DATABASE_URL)" $(UV) run pytest -m integration

test-cov: ## Run all tests with coverage report
	AEROZA_TEST_DATABASE_URL="$(TEST_DATABASE_URL)" $(UV) run pytest \
		--cov=aeroza --cov-report=term-missing --cov-report=html

lint: ## Run ruff lint
	$(UV) run ruff check .

format: ## Run ruff formatter
	$(UV) run ruff format .

typecheck: ## Run mypy
	$(UV) run mypy aeroza

check: lint typecheck test ## Run lint, type-check, and unit tests

migrate: ## Apply all pending migrations to the dev database
	$(UV) run alembic upgrade head

migrate-down: ## Roll back one migration on the dev database
	$(UV) run alembic downgrade -1

migration: ## Generate a new migration: `make migration MSG="add fields"`
	@if [ -z "$(MSG)" ]; then echo "MSG is required, e.g. make migration MSG=\"…\""; exit 2; fi
	$(UV) run alembic revision --autogenerate -m "$(MSG)"

db-shell: ## psql into the dev database
	docker compose exec postgres psql -U aeroza -d aeroza

clean: ## Remove caches and build artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov dist build *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# --- Dev console (Next.js) -------------------------------------------------

web-install: ## Install dev-console npm dependencies
	cd web && npm install

web-dev: ## Run the Next.js dev console on :3000 (needs `make dev` running)
	cd web && npm run dev

web-build: ## Production-build the dev console
	cd web && npm run build

web-typecheck: ## Type-check the dev console
	cd web && npm run typecheck
