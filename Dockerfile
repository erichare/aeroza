# syntax=docker/dockerfile:1.7
# Multi-stage build for the Aeroza API + workers.
#
# Single-image, multi-process layout: one container runs uvicorn, the
# alerts/MRMS/METAR pollers, the GRIB→Zarr materialiser, and the webhook
# dispatcher under honcho. This is what makes a single Railway service
# (one container, one volume) viable — the materialiser writes Zarr
# grids to ``/app/data`` and the API reads tiles from the same path,
# which can't be split across services because Railway volumes don't
# share.

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

# Build deps + libeccodes-dev (the C library cfgrib needs to decode
# GRIB2). Without eccodes the materialiser worker fails on every grid
# and the radar tiles never render.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libeccodes-dev \
    && rm -rf /var/lib/apt/lists/*

# LICENSE + README are referenced from pyproject.toml so they have to
# land in the build context before any `uv sync` step that tries to
# install the project itself (otherwise hatchling raises on missing
# license file).
COPY pyproject.toml uv.lock* LICENSE README.md ./
# Resolve the full production dep set: db (alembic + asyncpg +
# sqlalchemy + geoalchemy2), cache (redis), stream (nats-py), ingest
# (boto3, xarray, zarr, numpy, pillow), grib (cfgrib — radar replay),
# verify (pandas — calibration scoring). `--no-dev` keeps test/lint
# tools out of the image.
ARG EXTRAS="--extra db --extra cache --extra stream --extra ingest --extra grib --extra verify"
RUN uv sync --frozen --no-install-project --no-dev ${EXTRAS} \
    || uv sync --no-install-project --no-dev ${EXTRAS}

COPY aeroza ./aeroza
COPY alembic ./alembic
COPY alembic.ini ./
RUN uv sync --frozen --no-dev ${EXTRAS} || uv sync --no-dev ${EXTRAS}

# honcho is in the dev group of pyproject.toml because nothing in the
# Python runtime imports it — it's only invoked from railway-start.sh
# to fan out into the worker process list. Install separately so we
# don't have to ship the rest of the dev tooling.
RUN uv pip install --python /app/.venv/bin/python honcho>=2.0

# ---- runtime ----
FROM base AS runtime

WORKDIR /app

# eccodes runtime library (the *-dev package above ships headers we
# only need at build time; the runtime image just needs libeccodes0).
RUN apt-get update \
    && apt-get install -y --no-install-recommends libeccodes0 \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user.
RUN groupadd --system --gid 1001 aeroza \
    && useradd --system --uid 1001 --gid aeroza --create-home aeroza

COPY --from=builder --chown=aeroza:aeroza /app /app
COPY --chown=aeroza:aeroza Procfile.railway scripts/railway-start.sh ./
RUN chmod +x ./railway-start.sh

# Materialised Zarr grids land here. Railway mounts a volume at this
# path so they survive deploys. ``aeroza`` user must own it before the
# materialiser tries to write.
RUN mkdir -p /app/data && chown -R aeroza:aeroza /app/data

ENV PATH="/app/.venv/bin:${PATH}" \
    AEROZA_DATA_DIR="/app/data"

USER aeroza
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health').read()" || exit 1

# Default to the multi-process Railway entrypoint. Override at run time
# (e.g. for a CI smoke test) with ``--entrypoint`` or by passing a
# different command.
CMD ["./railway-start.sh"]
