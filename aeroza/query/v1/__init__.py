"""Aeroza v1 query API — composed from per-domain sub-routers.

The previous monolithic ``aeroza/query/v1.py`` (1,317 lines) was split into
focused modules under this package. The wire surface is identical; the
``/v1/*`` paths and OpenAPI schema are unchanged.

Per-domain modules:

- :mod:`aeroza.query.v1.alerts` — ``/v1/alerts*`` (active, stream, historical, detail).
- :mod:`aeroza.query.v1.mrms` — ``/v1/mrms/*`` (files, grids, tiles, sample, polygon).
- :mod:`aeroza.query.v1.nowcasts` — ``/v1/nowcasts``.
- :mod:`aeroza.query.v1.stats` — ``/v1/stats``.
- :mod:`aeroza.query.v1.calibration` — ``/v1/calibration*``.
- :mod:`aeroza.query.v1.metar` — ``/v1/metar*``.

Auth (``GET /v1/me``) lives in :mod:`aeroza.auth.routes`. Webhook + alert-rule
CRUD live in :mod:`aeroza.webhooks.routes` and :mod:`aeroza.webhooks.rule_routes`.

Each sub-module exposes a bare ``APIRouter`` (no prefix); this package mounts
them all under ``/v1``. Within each module, route registration order is
preserved so literal paths shadow path-parameter matchers correctly
(``/alerts/stream`` and ``/alerts/historical`` register before
``/alerts/{alert_id}``; ``/mrms/grids/sample`` and ``/mrms/grids/polygon``
register before ``/mrms/grids/{file_key}``).
"""

from __future__ import annotations

from fastapi import APIRouter

from aeroza.query.v1.alerts import _alert_event_stream  # re-exported for tests
from aeroza.query.v1.alerts import router as _alerts_router
from aeroza.query.v1.calibration import router as _calibration_router
from aeroza.query.v1.metar import router as _metar_router
from aeroza.query.v1.mrms import router as _mrms_router
from aeroza.query.v1.nowcasts import router as _nowcasts_router
from aeroza.query.v1.stats import router as _stats_router

router = APIRouter(prefix="/v1")
router.include_router(_alerts_router)
router.include_router(_mrms_router)
router.include_router(_nowcasts_router)
router.include_router(_stats_router)
router.include_router(_calibration_router)
router.include_router(_metar_router)

__all__ = ["_alert_event_stream", "router"]
