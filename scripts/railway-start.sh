#!/usr/bin/env bash
# Railway entrypoint: run alembic, then hand off to honcho with the
# api + workers process list.
#
# Why a wrapper instead of CMD-ing honcho directly:
#
#   1. Migrations have to run before the API starts serving traffic.
#      Putting `alembic upgrade head` in CMD would race the workers,
#      half of which open DB connections in their startup hooks. One
#      sequential step here keeps the order deterministic.
#
#   2. We want a single "stop everything on first crash" semantics so
#      Railway's restart policy sees a clean exit. honcho already does
#      that, so we just exec into it and let it own PID 1.
#
#   3. honcho's process management is simpler than supervisord and
#      doesn't need a config file — the Procfile is the config.

set -euo pipefail

cd /app

echo "[boot] applying database migrations…"
alembic upgrade head

echo "[boot] starting api + workers via honcho…"
exec honcho -f Procfile.railway start
