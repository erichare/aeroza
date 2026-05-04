"""Read-only proxy for historical NWS Storm-Based Warnings via IEM.

NWS's own ``api.weather.gov/alerts`` endpoint only retains alerts for the
last few weeks. The /demo Storm Replay surfaces events from 2021–2024,
which are well outside that window. The Iowa Environmental Mesonet (IEM)
archives every NWS warning polygon back to 2002 and exposes them via a
small JSON API — that's what this module wraps.

Output is normalised into the same :class:`AlertFeature` /
:class:`AlertFeatureCollection` shapes that ``/v1/alerts`` returns, so
the front-end can render historical and live alerts through the same
MapLibre layers without a code-path branch.

Caching is in-process LRU on ``(since, until, wfos)`` because:

* IEM responses are stable for any past time window (the archive doesn't
  change), so we never need to invalidate.
* A single replay frame swap shouldn't trigger an upstream fetch — the
  page hits this endpoint once per event selection.
* Memory cost is bounded: 7 featured events × a few hundred KB each.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from aeroza.query.schemas import AlertFeature, AlertFeatureCollection, AlertProperties

log = logging.getLogger(__name__)

# IEM's storm-based-warnings JSON API. The endpoint we want is documented
# at https://mesonet.agron.iastate.edu/api/ — the GeoJSON variant is the
# same as the JSON variant but wraps each row in a Feature with a real
# polygon geometry, which is what the map needs.
IEM_SBW_URL = (
    "https://mesonet.agron.iastate.edu/api/1/vtec/sbw_interval.geojson"
)

# Hard cap on the IEM round-trip. The endpoint typically replies in
# 0.5–3s for a single-WFO query; 15s is a conservative ceiling that
# still surfaces a 504 to the user instead of hanging forever.
IEM_TIMEOUT_SECONDS = 15.0

# IEM's `phenomena` 2-letter codes mapped to our Severity ladder. Tornado
# / Extreme Wind sit at the top because they're the only categories
# routinely upgraded to "PDS" (Particularly Dangerous Situation) language;
# Severe Thunderstorm + Flash Flood are the workhorse warnings; others
# fall through to Moderate. The mapping is intentionally coarse — the
# UI's severity ladder only has five rungs.
_PHENOMENA_TO_SEVERITY: dict[str, str] = {
    "TO": "Extreme",   # Tornado
    "EW": "Extreme",   # Extreme Wind
    "HU": "Extreme",   # Hurricane
    "SS": "Extreme",   # Storm Surge
    "SV": "Severe",    # Severe Thunderstorm
    "FF": "Severe",    # Flash Flood
    "FA": "Severe",    # Areal Flood
    "MA": "Severe",    # Marine
    "TR": "Severe",    # Tropical Storm
    "SQ": "Severe",    # Snow Squall
    "DS": "Severe",    # Dust Storm
}


@dataclass(frozen=True, slots=True)
class HistoricalAlertQuery:
    """Inputs to :func:`fetch_historical_alerts`.

    ``wfos`` is required when going through the cache — passing an empty
    tuple to the IEM endpoint would return every WFO nationwide for the
    window, which is too much data to serve from one process. Callers
    that genuinely want CONUS-wide history should iterate WFOs and merge.
    """

    since: datetime
    until: datetime
    wfos: tuple[str, ...]


# Process-wide cache. IEM's archive is immutable for any past window so
# entries never need eviction; we cap at 64 to keep memory bounded if a
# malicious caller probes thousands of distinct windows.
_CACHE: dict[str, AlertFeatureCollection] = {}
_CACHE_MAX = 64
# Per-key locks coalesce concurrent first-misses for the same key — two
# /demo tabs selecting the same event simultaneously fire one IEM
# request, not two.
_CACHE_LOCKS: dict[str, asyncio.Lock] = {}
_LOCKS_GUARD = asyncio.Lock()


async def fetch_historical_alerts(query: HistoricalAlertQuery) -> AlertFeatureCollection:
    """Return historical Storm-Based Warnings for ``query`` as GeoJSON.

    Cached by ``(since, until, wfos)`` for the lifetime of the process —
    IEM's archive is immutable for any past window so a hit is always
    safe. The first miss for a given key fires one HTTP request to IEM
    while concurrent callers wait on a per-key lock.
    """
    key = _cache_key(query)
    cached = _CACHE.get(key)
    if cached is not None:
        return cached

    async with _LOCKS_GUARD:
        lock = _CACHE_LOCKS.setdefault(key, asyncio.Lock())

    async with lock:
        # Re-check inside the lock — another coroutine may have populated
        # the cache while we were waiting on it.
        cached = _CACHE.get(key)
        if cached is not None:
            return cached
        result = await _fetch_async(query)
        if len(_CACHE) >= _CACHE_MAX:
            # Drop the oldest entry — Python dicts preserve insertion
            # order so the first key is the least-recently inserted.
            _CACHE.pop(next(iter(_CACHE)))
        _CACHE[key] = result
        return result


def _cache_key(query: HistoricalAlertQuery) -> str:
    return (
        f"{query.since.isoformat()}|{query.until.isoformat()}|"
        f"{','.join(sorted(query.wfos))}"
    )


# IEM caps each request to three WFOs. Larger event regions span more
# offices, so the backend chunks the list and fan-outs concurrent
# requests, then merges the results. Three keeps each round-trip small
# enough to stay well inside the 15-second timeout.
IEM_MAX_WFOS_PER_REQUEST = 3


async def _fetch_async(query: HistoricalAlertQuery) -> AlertFeatureCollection:
    chunks = [
        query.wfos[i : i + IEM_MAX_WFOS_PER_REQUEST]
        for i in range(0, len(query.wfos), IEM_MAX_WFOS_PER_REQUEST)
    ]
    async with httpx.AsyncClient(timeout=IEM_TIMEOUT_SECONDS) as client:
        results = await asyncio.gather(
            *(_fetch_chunk(client, query.since, query.until, chunk) for chunk in chunks),
            return_exceptions=False,
        )

    # Merge chunk results, deduping by feature id — a warning whose
    # polygon shrinks via update could end up in two chunks if the
    # affected counties span multiple WFOs (rare but possible at
    # office boundaries).
    seen: set[str] = set()
    features: list[AlertFeature] = []
    for chunk_result in results:
        for feat in chunk_result.features:
            if feat.properties.id in seen:
                continue
            seen.add(feat.properties.id)
            features.append(feat)
    return AlertFeatureCollection(features=features)


async def _fetch_chunk(
    client: httpx.AsyncClient,
    since: datetime,
    until: datetime,
    wfos: tuple[str, ...],
) -> AlertFeatureCollection:
    params: dict[str, Any] = {
        "begints": _format_iso(since),
        "endts": _format_iso(until),
        # The IEM endpoint accepts repeated `wfo` params for multi-WFO
        # filtering (capped at 3 per call).
        "wfo": list(wfos),
    }
    try:
        response = await client.get(IEM_SBW_URL, params=params)
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPError as exc:
        log.warning("iem.fetch_failed", exc_info=exc)
        return AlertFeatureCollection(features=[])
    except json.JSONDecodeError as exc:
        log.warning("iem.parse_failed", exc_info=exc)
        return AlertFeatureCollection(features=[])

    raw_features = payload.get("features", [])
    if not isinstance(raw_features, list):
        return AlertFeatureCollection(features=[])
    features = [
        feat
        for feat in (_normalise_feature(raw) for raw in raw_features)
        if feat is not None
    ]
    return AlertFeatureCollection(features=features)


def _format_iso(dt: datetime) -> str:
    # IEM accepts trailing Z (no offset suffix). Strip the Python
    # +00:00 form so the URL is the canonical UTC shape.
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalise_feature(raw: Any) -> AlertFeature | None:
    if not isinstance(raw, dict):
        return None
    properties = raw.get("properties")
    geometry = raw.get("geometry")
    if not isinstance(properties, dict):
        return None
    product_id = properties.get("product_id")
    if not isinstance(product_id, str):
        # Without a stable id we can't dedupe or feature-state-track.
        return None
    phenomena = str(properties.get("phenomena") or "").upper()
    event_label = properties.get("event_label") or phenomena or "Unknown event"
    severity = _PHENOMENA_TO_SEVERITY.get(phenomena, "Moderate")

    # Polygon update timestamps are more accurate than the issuance pair
    # for replay rendering — they reflect when the polygon was actually
    # in force on screen, which is what the /demo scrubber compares
    # against. Fall back to issuance times when polygon-specific times
    # are absent.
    onset = _parse_dt(
        properties.get("utc_polygon_begin") or properties.get("utc_issue")
    )
    ends = _parse_dt(
        properties.get("utc_polygon_end") or properties.get("utc_expire")
    )

    wfo = properties.get("wfo")
    sender_name = f"NWS {wfo}" if isinstance(wfo, str) and wfo else None

    return AlertFeature(
        geometry=geometry if isinstance(geometry, dict) else None,
        properties=AlertProperties(
            id=product_id,
            event=str(event_label),
            headline=str(event_label),
            severity=severity,
            urgency="Immediate",   # SBWs are always immediate by definition
            certainty="Observed",  # …and observed (not forecast)
            sender_name=sender_name,
            area_desc=_string_or_none(properties.get("locations")),
            effective=onset,
            onset=onset,
            expires=ends,
            ends=ends,
        ),
    )


def _parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        # IEM emits "2024-05-16T22:02:00Z" — fromisoformat in 3.11+
        # parses the trailing Z natively.
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def parse_wfo_list(raw: str | None) -> tuple[str, ...]:
    """Parse a comma-separated WFO list into uppercase 3-letter codes.

    Empty / whitespace items are dropped silently. The route-layer
    validator should reject a fully-empty list before calling this — but
    if it slips through, the upstream IEM call will return a 400 we
    then catch and convert to an empty FeatureCollection.
    """
    if raw is None:
        return ()
    return tuple(
        item.strip().upper() for item in raw.split(",") if item.strip()
    )


def normalise_wfos(items: Sequence[str]) -> tuple[str, ...]:
    """Trim + uppercase a sequence of WFOs into a deterministic tuple."""
    return tuple(item.strip().upper() for item in items if item.strip())
