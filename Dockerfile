# syntax=docker/dockerfile:1.7
# Multi-stage build for the Aeroza API.

ARG PYTHON_VERSION=3.13
FROM python:${PYTHON_VERSION}-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# ---- builder ----
FROM base AS builder

# uv for fast, reproducible installs
COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /uvx /usr/local/bin/

WORKDIR /app

# Install build dependencies for any wheels that need compilation.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-install-project --no-dev || uv sync --no-install-project --no-dev

COPY aeroza ./aeroza
RUN uv sync --frozen --no-dev || uv sync --no-dev

# ---- runtime ----
FROM base AS runtime

WORKDIR /app

# Non-root runtime user.
RUN groupadd --system --gid 1001 aeroza \
    && useradd --system --uid 1001 --gid aeroza --create-home aeroza

COPY --from=builder --chown=aeroza:aeroza /app /app

ENV PATH="/app/.venv/bin:${PATH}"

USER aeroza
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health').read()" || exit 1

CMD ["uvicorn", "aeroza.main:app", "--host", "0.0.0.0", "--port", "8000"]
