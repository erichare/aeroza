"""Point + polygon sampling against materialised MRMS Zarr grids.

The :func:`sample_grid_at_point` function takes a locator (Zarr URI +
variable name) plus a (lat, lng) and returns the value at the nearest
grid cell — the read-side primitive that lifts the catalog from "we
know what's available" to "we can actually answer questions about it".

:func:`sample_grid_in_polygon` extends that to a polygon: cells whose
centres fall inside the polygon are reduced via ``max`` / ``mean`` /
``min`` / ``count_ge`` (count of cells at-or-above a threshold). That
last one is the wedge for "is anything intense enough inside this
polygon, right now?" — the building block for alerting and geofencing.

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
- **Polygon clipping**: the polygon's bounding box is used to slice
  the grid down before masking, so a small polygon over a CONUS-scale
  grid only loads a tiny chunk off disk. The mask itself is a
  vectorised ray-cast against cell centres.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Final, Literal

import numpy as np
import structlog
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:  # pragma: no cover - typing only
    import xarray as xr

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


# ---------------------------------------------------------------------------
# Polygon reducer
# ---------------------------------------------------------------------------


PolygonReducer = Literal["max", "mean", "min", "count_ge"]
ALL_REDUCERS: Final[tuple[PolygonReducer, ...]] = ("max", "mean", "min", "count_ge")


@dataclass(frozen=True, slots=True)
class PolygonSample:
    """Reduced value over the cells of a grid that fall inside a polygon.

    ``cell_count`` is the number of cells whose centres are inside the
    polygon (the size of the masked region). ``threshold`` is the
    threshold used by the ``count_ge`` reducer; ``None`` for the others.
    """

    reducer: PolygonReducer
    value: float
    cell_count: int
    variable: str
    threshold: float | None
    bbox_min_latitude: float
    bbox_min_longitude: float
    bbox_max_latitude: float
    bbox_max_longitude: float


async def sample_grid_in_polygon(
    *,
    zarr_uri: str,
    variable: str,
    polygon_lng_lat: tuple[tuple[float, float], ...],
    reducer: PolygonReducer,
    threshold: float | None = None,
) -> PolygonSample:
    """Reduce ``variable`` over cells inside ``polygon_lng_lat`` via ``reducer``.

    ``polygon_lng_lat`` is a sequence of ``(lng, lat)`` pairs in WGS84;
    closure is implicit (the last edge connects back to the first
    vertex). ``threshold`` is required when ``reducer == "count_ge"``
    and ignored otherwise.

    Raises:
        OutOfDomainError: when no grid cell centres fall inside the polygon.
        FileNotFoundError: when the Zarr store is missing.
        KeyError: when ``variable`` is not in the store.
        ValueError: when ``reducer == "count_ge"`` and ``threshold`` is None.
    """
    if reducer == "count_ge" and threshold is None:
        raise ValueError("reducer 'count_ge' requires a numeric threshold")
    return await asyncio.to_thread(
        _sample_polygon_sync,
        zarr_uri=zarr_uri,
        variable=variable,
        polygon_lng_lat=polygon_lng_lat,
        reducer=reducer,
        threshold=threshold,
    )


def _sample_polygon_sync(
    *,
    zarr_uri: str,
    variable: str,
    polygon_lng_lat: tuple[tuple[float, float], ...],
    reducer: PolygonReducer,
    threshold: float | None,
) -> PolygonSample:
    import xarray as xr

    ds = xr.open_zarr(zarr_uri)
    try:
        if variable not in ds.variables:
            raise KeyError(f"variable {variable!r} not in store {zarr_uri}")
        da = ds[variable]

        # Translate the polygon onto the grid's longitude convention so the
        # bbox slice and the mask both compare apples to apples.
        polygon_grid = _polygon_to_grid_convention(da, polygon_lng_lat)
        clipped = _clip_to_polygon_bbox(da, polygon_grid)
        if clipped.size == 0:
            raise OutOfDomainError(f"polygon does not overlap the grid extent of {zarr_uri}")

        mask = _polygon_mask(clipped, polygon_grid)
        cell_count = int(mask.sum())
        if cell_count == 0:
            raise OutOfDomainError(
                "no grid cell centres fall inside the polygon "
                f"(bbox covered {clipped.size} cells in {zarr_uri})"
            )

        values = clipped.values[mask]
        result = _apply_reducer(values, reducer, threshold)

        # Reported bbox is the grid's actual coverage of the polygon, with
        # longitudes back on the [-180, 180] convention.
        lat_axis = clipped["latitude"].values
        lng_axis = clipped["longitude"].values
        return PolygonSample(
            reducer=reducer,
            value=float(result),
            cell_count=cell_count,
            variable=variable,
            threshold=threshold,
            bbox_min_latitude=float(lat_axis.min()),
            bbox_min_longitude=_to_signed_longitude(float(lng_axis.min())),
            bbox_max_latitude=float(lat_axis.max()),
            bbox_max_longitude=_to_signed_longitude(float(lng_axis.max())),
        )
    finally:
        ds.close()


def _polygon_to_grid_convention(
    da: xr.DataArray,
    polygon_lng_lat: tuple[tuple[float, float], ...],
) -> np.ndarray:
    """Translate every vertex's longitude into the grid's convention.

    Returns an ``(N, 2)`` numpy array of ``(lng, lat)`` pairs ready for
    bbox-clipping and ray-casting.
    """
    arr = np.asarray(polygon_lng_lat, dtype=np.float64)
    longitudes = getattr(da, "longitude", None)
    if longitudes is None:
        return arr
    try:
        max_lng = float(longitudes.max().item())
    except Exception:  # pragma: no cover - defensive
        return arr
    if max_lng > 180.0:
        arr[:, 0] = np.where(arr[:, 0] < 0.0, arr[:, 0] + 360.0, arr[:, 0])
    return arr


def _clip_to_polygon_bbox(da: xr.DataArray, polygon: np.ndarray) -> xr.DataArray:
    """Return ``da`` sliced to the polygon's lat/lng bounding box.

    Handles axes that ascend or descend (MRMS tiles can be either) by
    inspecting the index direction and flipping the slice when needed.
    Loading only the bbox subset keeps polygon queries cheap on
    CONUS-scale grids.
    """
    min_lng, min_lat = polygon.min(axis=0)
    max_lng, max_lat = polygon.max(axis=0)

    lat_axis = da["latitude"].values
    lng_axis = da["longitude"].values

    lat_slice = slice(min_lat, max_lat) if lat_axis[0] <= lat_axis[-1] else slice(max_lat, min_lat)
    lng_slice = slice(min_lng, max_lng) if lng_axis[0] <= lng_axis[-1] else slice(max_lng, min_lng)

    clipped = da.sel(latitude=lat_slice, longitude=lng_slice)
    # Materialise the (small) clipped subset eagerly so subsequent numpy
    # work doesn't re-trigger Zarr reads.
    return clipped.load()


def _polygon_mask(da: xr.DataArray, polygon: np.ndarray) -> np.ndarray:
    """2D boolean mask: cells whose centres lie inside ``polygon``.

    Vectorised even-odd ray-casting against cell centres. Polygon is
    treated as implicitly closed (last vertex connects to first).
    """
    lat_axis = da["latitude"].values
    lng_axis = da["longitude"].values
    XX, YY = np.meshgrid(lng_axis, lat_axis, indexing="xy")  # both (n_lat, n_lng)

    px = polygon[:, 0]
    py = polygon[:, 1]
    n = len(polygon)
    inside = np.zeros_like(XX, dtype=bool)

    j = n - 1
    for i in range(n):
        xi, yi = px[i], py[i]
        xj, yj = px[j], py[j]
        # Avoid division by zero on horizontal edges; the ((y_i > Y) != (y_j > Y))
        # gate already filters those out, but the denominator must be safe.
        denom = yj - yi
        if denom == 0:
            j = i
            continue
        edge_x = (xj - xi) * (YY - yi) / denom + xi
        crossing = ((yi > YY) != (yj > YY)) & (edge_x > XX)
        inside ^= crossing
        j = i
    return inside


def _apply_reducer(
    values: np.ndarray,
    reducer: PolygonReducer,
    threshold: float | None,
) -> float:
    if values.size == 0:
        # Caller already raises OutOfDomainError before getting here.
        raise OutOfDomainError("reducer received empty cell set")
    if reducer == "max":
        return float(values.max())
    if reducer == "min":
        return float(values.min())
    if reducer == "mean":
        return float(values.mean())
    if reducer == "count_ge":
        assert threshold is not None  # guarded at the public entry point
        return float((values >= threshold).sum())
    raise ValueError(f"unknown reducer {reducer!r}")  # pragma: no cover - typing


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


class MrmsGridPolygonResponse(BaseModel):
    """Wire shape returned by ``GET /v1/mrms/grids/polygon``.

    Carries the reducer's value, the count of cells that landed inside
    the polygon, and the grid's bounding box of those cells. For
    ``count_ge``, ``threshold`` echoes the request and ``value`` is the
    count expressed as a float (so the JSON shape matches the other
    reducers and downstream parsers don't have to branch on type).
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    type: Literal["MrmsGridPolygonSample"] = "MrmsGridPolygonSample"
    file_key: str = Field(serialization_alias="fileKey")
    product: str
    level: str
    valid_at: datetime = Field(serialization_alias="validAt")
    variable: str
    reducer: PolygonReducer
    threshold: float | None
    value: float
    cell_count: int = Field(serialization_alias="cellCount")
    vertex_count: int = Field(serialization_alias="vertexCount")
    bbox_min_latitude: float = Field(serialization_alias="bboxMinLatitude")
    bbox_min_longitude: float = Field(serialization_alias="bboxMinLongitude")
    bbox_max_latitude: float = Field(serialization_alias="bboxMaxLatitude")
    bbox_max_longitude: float = Field(serialization_alias="bboxMaxLongitude")


__all__ = [
    "ALL_REDUCERS",
    "DEFAULT_TOLERANCE_DEG",
    "MAX_TOLERANCE_DEG",
    "GridSample",
    "MrmsGridPolygonResponse",
    "MrmsGridSampleResponse",
    "OutOfDomainError",
    "PolygonReducer",
    "PolygonSample",
    "sample_grid_at_point",
    "sample_grid_in_polygon",
]
