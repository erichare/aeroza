.PHONY: help doctor bootstrap start stop install dev up down logs test test-unit test-integration test-cov \
        lint format format-check typecheck check migrate migrate-down migration db-shell clean \
        web-install web-dev web-build web-typecheck \
        ingest-alerts ingest-mrms ingest-metar materialise-mrms \
        nowcast-pysteps nowcast-persistence nowcast-lagged-ensemble \
        seed extras-grib extras-nowcast

UV ?= uv

# DSN used by the integration test suite. Override per-environment as needed
# (e.g. `make test-integration TEST_DATABASE_URL=postgresql+asyncpg://...`).
TEST_DATABASE_URL ?= postgresql+asyncpg://aeroza:aeroza@localhost:5432/aeroza_test

# Extras that the dev stack needs to boot. Anything outside this set is
# heavy / system-dep-bearing and stays opt-in via the targets below.
# `uv sync --extra X` REPLACES the installed extra-set rather than adding
# to it, so any future "add an extra" target has to re-list these too.
BOOTSTRAP_EXTRAS = --extra db --extra cache --extra stream --extra ingest --extra verify

help:
	@awk 'BEGIN {FS = ":.*##"; printf "Aeroza make targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# --- One-command setup -----------------------------------------------------
# `make start` is the friction-free path: doctor → bootstrap → run the
# whole stack with honcho. `make stop` brings the docker layer down.
# Power users can still drive each piece individually below.

doctor: ## Pre-flight: check that uv, docker, node are installed
	@missing=""; \
	for cmd in uv docker node npm; do \
		if ! command -v $$cmd >/dev/null 2>&1; then missing="$$missing $$cmd"; fi; \
	done; \
	if ! docker compose version >/dev/null 2>&1; then missing="$$missing docker-compose-plugin"; fi; \
	if [ -n "$$missing" ]; then \
		printf "\033[31mMissing tools:\033[0m%s\n\n" "$$missing"; \
		printf "  uv      → https://docs.astral.sh/uv/\n"; \
		printf "  docker  → https://docs.docker.com/get-docker/\n"; \
		printf "  node    → https://nodejs.org/ (or nvm)\n"; \
		exit 1; \
	fi; \
	printf "\033[32mAll required tools present.\033[0m\n"

bootstrap: doctor ## First-run setup: .env, deps, infra, migrations (idempotent)
	@if [ ! -f .env ]; then \
		printf "Creating .env from .env.example…\n"; \
		cp .env.example .env; \
		salt=$$(python3 -c 'import secrets; print(secrets.token_hex(32))'); \
		sed -e "s|AEROZA_API_KEY_SALT=.*|AEROZA_API_KEY_SALT=$$salt|" .env > .env.tmp && mv .env.tmp .env; \
		printf "  → generated AEROZA_API_KEY_SALT\n"; \
	fi
	@if ! $(UV) run --quiet python -c "import alembic, fastapi, sqlalchemy, redis, nats" >/dev/null 2>&1; then \
		printf "Syncing Python deps…\n"; \
		$(UV) sync --quiet $(BOOTSTRAP_EXTRAS); \
	fi
	@if [ ! -f node_modules/.package-lock.json ]; then \
		printf "Installing web deps…\n"; \
		npm install --silent; \
	fi
	@printf "Bringing up Postgres / Redis / NATS…\n"
	@docker compose up -d
	@printf "Waiting for Postgres to accept connections"
	@for i in $$(seq 1 60); do \
		if docker compose exec -T postgres pg_isready -U aeroza -q 2>/dev/null; then \
			printf " ok\n"; break; \
		fi; \
		printf "."; sleep 1; \
		if [ $$i -eq 60 ]; then printf "\n\033[31mPostgres did not become ready in 60s\033[0m\n"; exit 1; fi; \
	done
	@printf "Applying migrations…\n"
	@$(UV) run --quiet alembic upgrade head
	@printf "\n\033[32mReady.\033[0m Run \033[36mmake start\033[0m to launch the stack.\n"

start: bootstrap ## Boot the whole stack (API + web + workers) in one terminal
	@for port in 8000 3000; do \
		if lsof -nP -iTCP:$$port -sTCP:LISTEN -t >/dev/null 2>&1; then \
			pid=$$(lsof -nP -iTCP:$$port -sTCP:LISTEN -t | head -1); \
			printf "\033[31mPort %s is already in use (pid %s).\033[0m\n" "$$port" "$$pid"; \
			printf "  Run \033[36mkill %s\033[0m or \033[36mmake stop\033[0m, then retry.\n" "$$pid"; \
			exit 1; \
		fi; \
	done
	@./scripts/start-stack.sh

seed: ## Backfill ~3h of historical MRMS data so the dashboard isn't empty (idempotent)
	@./scripts/seed-historical.sh

stop: ## Stop the docker compose stack (Postgres, Redis, NATS)
	@docker compose down

# --- Optional extras ------------------------------------------------------
# `uv sync --extra X` REPLACES the installed extra-set, not adds to it.
# Plain `uv sync --extra grib` would silently uninstall db / cache /
# stream / ingest / verify and leave the stack broken. These targets
# re-list the bootstrap extras so adding grib (or nowcast) on top of an
# already-bootstrapped venv stays additive in effect.
#
# Both extras need a system library too — handled outside uv:
#   grib    → eccodes (brew install eccodes / apt-get install -y libeccodes-dev)
#   nowcast → libomp on macOS (brew install libomp); Linux works out of the box

extras-grib: ## Install the [grib] extra (cfgrib) on top of bootstrap deps
	@printf "Syncing Python deps with [grib] extra…\n"
	@$(UV) sync $(BOOTSTRAP_EXTRAS) --extra grib
	@printf "\033[32mDone.\033[0m The materialiser can now decode GRIB2 → Zarr.\n"
	@printf "  → Run \033[36maeroza-materialise-mrms --once\033[0m to process queued files.\n"

extras-nowcast: ## Install the [nowcast] extra (pySTEPS) on top of bootstrap deps
	@printf "Syncing Python deps with [nowcast] extra…\n"
	@printf "  Note: macOS needs \033[36mbrew install libomp\033[0m first.\n"
	@$(UV) sync $(BOOTSTRAP_EXTRAS) --extra nowcast
	@printf "\033[32mDone.\033[0m pySTEPS is available as a forecaster.\n"
	@printf "  → Run \033[36mmake nowcast-pysteps\033[0m to use it.\n"

install: ## Sync Python dependencies via uv (every extra; needs eccodes + libomp)
	$(UV) sync --all-extras

dev: ## Run the API with hot reload
	$(UV) run uvicorn aeroza.main:app --reload --host 0.0.0.0 --port 8000

ingest-alerts: ## Long-running NWS alerts poller (run alongside `make dev`)
	$(UV) run aeroza-ingest-alerts

ingest-mrms: ## Long-running MRMS file-catalog poller (run alongside `make dev`)
	$(UV) run aeroza-ingest-mrms

ingest-metar: ## Long-running METAR poller (run alongside `make dev`)
	$(UV) run aeroza-ingest-metar

materialise-mrms: ## Long-running MRMS Zarr materialiser (run alongside `make dev`)
	$(UV) run aeroza-materialise-mrms

nowcast-pysteps: ## Long-running nowcaster using pySTEPS (needs --extra nowcast)
	$(UV) run aeroza-nowcast-mrms --algorithm pysteps

nowcast-persistence: ## Long-running nowcaster using the persistence baseline
	$(UV) run aeroza-nowcast-mrms --algorithm persistence

nowcast-lagged-ensemble: ## Long-running nowcaster using the lagged-ensemble forecaster (no extras needed)
	$(UV) run aeroza-nowcast-mrms --algorithm lagged-ensemble

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

format-check: ## Verify formatter is a no-op (CI parity)
	$(UV) run ruff format --check .

typecheck: ## Run mypy
	$(UV) run mypy aeroza

check: lint format-check typecheck test ## Run lint, format-check, type-check, and unit tests

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
