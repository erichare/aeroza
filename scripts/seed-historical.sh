#!/usr/bin/env bash
# Seed the local stack with a few hours of historical MRMS data so the
# dashboard isn't empty on cold start. Idempotent — if the file catalog
# already has data, this exits without doing anything.
#
# Tiered behaviour, each tier degrades gracefully:
#
#   1. Always: ingest historical MRMS file catalog (`aeroza-ingest-mrms
#      --at-time …`). Lights up GET /v1/mrms/files even on a vanilla
#      install with no GRIB extras.
#
#   2. With cfgrib (`make extras-grib`): drain the materialiser in
#      `--once` batches until no unmaterialised files remain. Lights up
#      GET /v1/mrms/grids and the radar replay.
#
#   3. With the long-running nowcast + verify workers in honcho's
#      Procfile (the dynamic start-stack script wires them in when
#      cfgrib is detected): the materialised-grid events flow through
#      NATS and Brier/CRPS calibration starts populating within a
#      couple of minutes.
#
# Run independently with `make seed`, or backgrounded automatically by
# `scripts/start-stack.sh` when the catalog is empty.

set -euo pipefail

# Anchor to the repo root so `make seed` and a backgrounded launch
# from `scripts/start-stack.sh` both pick up `.env` and the `uv` venv
# the same way.
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
cd "$REPO_ROOT"

# ----- Knobs -----
# How far back to backfill. 3h gives the materialiser ~30 grids on
# MergedReflectivityComposite cadence (~one every 6 minutes), enough
# to populate the replay scrubber with a sensible swept-window.
SEED_HOURS="${AEROZA_SEED_HOURS:-3}"

# Below this row count we treat the catalog as empty and run the seed.
# Bigger than 0 so an aborted partial seed re-runs cleanly; smaller
# than the count of a real run (~30+ files in 3h) so we never seed
# again once the live ingest has drifted past us.
SKIP_IF_FILES_GE="${AEROZA_SEED_SKIP_IF_FILES_GE:-20}"

# Materialise batch size for `--once`. The CLI's own default (50) is
# tuned for live polling cadence; backfills want more per batch. Loop
# until the count of pending files stops decreasing.
MATERIALISE_BATCH="${AEROZA_SEED_MATERIALISE_BATCH:-100}"

UV="${UV:-uv}"

# ----- Logging -----
log() {
  printf "[seed] %s\n" "$*"
}

# ----- Catalog probe -----
# Read counts via /v1/stats rather than psql to avoid coupling to the
# DSN format (asyncpg vs psycopg2 etc.). The endpoint returns a
# compact ``{mrms: {files, gridsMaterialised, filesPending, …}}``.
api_base="${AEROZA_API_URL:-http://localhost:8000}"

probe_stat() {
  # $1 is a dotted JSON path under mrms (e.g. "files", "filesPending").
  # Echoes the int value, or empty string when /v1/stats is unreachable.
  local key="$1"
  curl -fsS --max-time 3 "$api_base/v1/stats" 2>/dev/null \
    | python3 -c "import json, sys; print(json.load(sys.stdin).get('mrms',{}).get('$key', 0))" \
    2>/dev/null \
    || true
}

wait_for_api() {
  # Spinning wait so when this script is launched in the background by
  # `scripts/start-stack.sh` it doesn't race honcho's API process.
  # 60 seconds is generous on cold start.
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 1 "$api_base/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  log "API at $api_base never came up — skipping seed."
  return 1
}

# ----- Main -----
wait_for_api || exit 0

count=$(probe_stat files)
if [ -z "$count" ]; then
  log "Could not read /v1/stats — skipping."
  exit 0
fi

if [ "$count" -ge "$SKIP_IF_FILES_GE" ]; then
  log "Catalog already has $count file(s) — nothing to seed. (Override with AEROZA_SEED_SKIP_IF_FILES_GE.)"
  exit 0
fi

# ISO-8601 timestamp `SEED_HOURS` ago, UTC. macOS `date -v` differs
# from GNU `date -d`; fall back through both.
seed_at() {
  if date -u -v-"${SEED_HOURS}H" +"%Y-%m-%dT%H:00:00Z" 2>/dev/null; then
    return 0
  fi
  if date -u -d "${SEED_HOURS} hours ago" +"%Y-%m-%dT%H:00:00Z" 2>/dev/null; then
    return 0
  fi
  echo ""
}
at_time=$(seed_at)
if [ -z "$at_time" ]; then
  log "Could not compute seed timestamp — skipping."
  exit 0
fi

log "Backfilling MRMS file catalog at $at_time (~${SEED_HOURS}h ago)…"
if ! $UV run --quiet aeroza-ingest-mrms --at-time "$at_time" 2>&1 | sed 's/^/[seed][mrms-ingest] /'; then
  log "Historical ingest failed — leaving the dashboard with whatever ingest catches in real time."
  exit 0
fi

# Re-probe so the operator sees the count climb in the log even if we
# bail at the cfgrib check below.
post=$(probe_stat files)
log "Ingested historical files. Catalog now has ${post:-?} row(s)."

# Tier 2: drain the materialiser if cfgrib is installed.
if ! $UV run --quiet python -c "from aeroza.ingest.mrms_decode import ensure_cfgrib_available; ensure_cfgrib_available()" >/dev/null 2>&1; then
  log "cfgrib not installed — skipping materialise/nowcast/verify tier."
  log "  Install it with: \033[36mmake extras-grib\033[0m, then re-run \033[36mmake seed\033[0m."
  exit 0
fi

# Loop the materialiser until the pending count stops decreasing. Caps
# at 6 iterations to avoid runaways if the materialiser is somehow
# making no progress (it logs its own "N still pending" warnings).
prev_pending=-1
for iter in 1 2 3 4 5 6; do
  log "Materialising historical grids (batch ${iter}, batch_size=${MATERIALISE_BATCH})…"
  $UV run --quiet aeroza-materialise-mrms --once --batch-size "$MATERIALISE_BATCH" 2>&1 \
    | sed 's/^/[seed][materialise] /' || true
  pending=$(probe_stat filesPending)
  if [ -z "$pending" ] || [ "$pending" = "0" ]; then
    log "All historical grids materialised."
    break
  fi
  if [ "$pending" = "$prev_pending" ]; then
    log "Materialiser made no progress this batch ($pending still pending) — bailing."
    break
  fi
  prev_pending="$pending"
done

log "Seed complete. The lagged-ensemble nowcaster + verifier (running in honcho) will populate /v1/calibration as forecasts and observations match up — usually within a couple of minutes."
