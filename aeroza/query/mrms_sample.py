"""Point sampling against materialised MRMS Zarr grids.

The :func:`sample_grid_at_point` function takes a locator (Zarr URI +
variable name) plus a (lat, lng) and returns the value at the nearest
grid cell — the read-side primitive that lifts the catalog from "we
know what's available" to "we can actually answer questions about it".

Why this lives here, not next to :mod:`aeroza.ingest.mrms_zarr`: the
write-side module (the materialiser) deals in xarray objects produced by
cfgrib in-process; this module deals in xarray objects re-loaded from
the Zarr store. They share a path-naming convention but otherwise
operate on disjoint inputs and outputs.

Design notes:

- **Sync xarray, async caller**: xarray + Zarr are synchronous. Each
  sample wraps the sync work in ``asyncio.to_thread`` so the FastAPI
  event loop stays responsive while a request is reading a few KB off
  disk.
- **Longitude convention**: MRMS publishes on a ``[0, 360)`` grid; the
  rest of the world (and most user input) uses ``[-180, 180]``. We
  inspect the grid's actual longitude axis and translate user input
  on the fly so the route surface stays in the standard convention.
- **Tolerance**: bare ``method="nearest"`` happily returns a value
  miles from the request point if the request is outside the grid.
  We pass ``tolerance`` so out-of-domain queries surface as a clean
  ``OutOfDomainError`` that the route maps to 404.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal

import structlog
from pydantic import BaseModel, ConfigDict, Field

log = structlog.get_logger(__name__)

# MRMS native resolution is 0.01°. Anything farther than this from the
# request point is "no data here", not "your nearest cell is 50 miles
# away" — the route should 404, not lie.
DEFAULT_TOLERANCE_DEG: Final[float] = 0.05

# Hard ceiling on user-supplied tolerance — past 1° (~110 km) "nearest"
# stops meaning anything useful and we're papering over a domain mismatch.
MAX_TOLERANCE_DEG: Final[float] = 1.0


class OutOfDomainError(LookupError):
    """Raised when the requested (lat, lng) falls outside the grid (or
    farther than the tolerance from any cell). Distinct from a missing
    Zarr store / variable, which is a config error."""


@dataclass(frozen=True, slots=True)
class GridSample:
    """One sampled value plus the coords of the cell that produced it.

    ``latitude`` / ``longitude`` are the *grid's* values for the matched
    cell, normalised back to ``[-180, 180]`` longitude — useful for
    callers who want to confirm "you asked for X, you got cell Y".
    """

    value: float
    latitude: float
    longitude: float
    variable: str


async def sample_grid_at_point(
    *,
    zarr_uri: str,
    variable: str,
    latitude: float,
    longitude: float,
    tolerance_deg: float = DEFAULT_TOLERANCE_DEG,
) -> GridSample:
    """Return the nearest-cell value for ``(latitude, longitude)``.

    Raises:
        OutOfDomainError: when no cell is within ``tolerance_deg`` of the
            requested point on either axis.
        FileNotFoundError: when the Zarr store at ``zarr_uri`` is missing.
        KeyError: when ``variable`` is not in the Zarr store.
    """
    return await asyncio.to_thread(
        _sample_sync,
        zarr_uri=zarr_uri,
        variable=variable,
        latitude=latitude,
        longitude=longitude,
        tolerance_deg=tolerance_deg,
    )


def _sample_sync(
    *,
    zarr_uri: str,
    variable: str,
    latitude: float,
    longitude: float,
    tolerance_deg: float,
) -> GridSample:
    import xarray as xr

    ds = xr.open_zarr(zarr_uri)
    try:
        if variable not in ds.variables:
            raise KeyError(f"variable {variable!r} not in store {zarr_uri}")
        da = ds[variable]
        lng_axis = _longitude_in_grid_convention(da, longitude)
        try:
            sampled = da.sel(
                latitude=latitude,
                longitude=lng_axis,
                method="nearest",
                tolerance=tolerance_deg,
            )
        except KeyError as exc:
            raise OutOfDomainError(
                f"no cell within {tolerance_deg}° of "
                f"(lat={latitude}, lng={longitude}) in {zarr_uri}"
            ) from exc

        matched_lat = float(sampled["latitude"].item())
        matched_lng_grid = float(sampled["longitude"].item())
        return GridSample(
            value=float(sampled.item()),
            latitude=matched_lat,
            longitude=_to_signed_longitude(matched_lng_grid),
            variable=variable,
        )
    finally:
        ds.close()


def _longitude_in_grid_convention(da: object, lng: float) -> float:
    """If the grid uses ``[0, 360)`` longitudes (MRMS native), translate
    a ``[-180, 180]`` request into that convention.

    Inspecting the axis once per request is cheap (the longitude
    coordinate is loaded eagerly by xarray); doing it on the fly avoids
    a separate "what convention does this grid use?" config column.
    """
    longitudes = getattr(da, "longitude", None)
    if longitudes is None:
        # Synthetic / test grids without a longitude coord: pass through.
        return lng
    try:
        max_lng = float(longitudes.max().item())
    except Exception:  # pragma: no cover - defensive
        return lng
    if max_lng > 180.0 and lng < 0.0:
        return lng + 360.0
    return lng


def _to_signed_longitude(lng: float) -> float:
    """Normalise ``[0, 360)`` longitudes back to ``[-180, 180]`` for the wire."""
    if lng > 180.0:
        return lng - 360.0
    return lng


class MrmsGridSampleResponse(BaseModel):
    """Wire shape returned by ``GET /v1/mrms/grids/sample``.

    Carries both the value and the *actual* cell coordinates the value
    came from — callers that snap user input to a known cell (for
    deduplication, caching, downstream joins) need the matched coords,
    not just the requested ones. ``validAt`` and ``fileKey`` identify
    the source grid so a follow-up call to ``/v1/mrms/grids/{file_key}``
    can fetch the rest of the locator.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    type: Literal["MrmsGridSample"] = "MrmsGridSample"
    file_key: str = Field(serialization_alias="fileKey")
    product: str
    level: str
    valid_at: datetime = Field(serialization_alias="validAt")
    variable: str
    value: float
    requested_latitude: float = Field(serialization_alias="requestedLatitude")
    requested_longitude: float = Field(serialization_alias="requestedLongitude")
    matched_latitude: float = Field(serialization_alias="matchedLatitude")
    matched_longitude: float = Field(serialization_alias="matchedLongitude")
    tolerance_deg: float = Field(serialization_alias="toleranceDeg")


__all__ = [
    "DEFAULT_TOLERANCE_DEG",
    "MAX_TOLERANCE_DEG",
    "GridSample",
    "MrmsGridSampleResponse",
    "OutOfDomainError",
    "sample_grid_at_point",
]
