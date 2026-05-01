"""Zarr materialisation for MRMS grids.

This slice covers **storage** only — given an :class:`xarray.DataArray`,
write it to a Zarr store under a deterministic path derived from the
source S3 key. The next slice plugs in :mod:`cfgrib` to produce that
DataArray from a real MRMS GRIB2 payload.

Path layout
-----------
We mirror the source S3 layout, swapping the trailing ``.grib2(.gz)``
suffix for ``.zarr``::

    CONUS/MergedReflectivityComposite_00.50/20260501/MRMS_..._120000.grib2.gz
    →
    <target_root>/CONUS/MergedReflectivityComposite_00.50/20260501/MRMS_..._120000.zarr

That preserves traceability — given a row in ``mrms_grids`` you can
recover the source S3 key by reversing the suffix swap.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:  # pragma: no cover - typing only
    import xarray as xr

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class MrmsGridLocator:
    """Metadata describing one materialised Zarr grid.

    The locator is the wire/persistence shape; it carries everything a
    downstream consumer needs to find the grid (``zarr_uri``) and reason
    about its shape without re-opening it.
    """

    file_key: str
    zarr_uri: str
    variable: str
    dims: tuple[str, ...]
    shape: tuple[int, ...]
    dtype: str
    nbytes: int


def zarr_path_for(target_root: str | Path, file_key: str) -> Path:
    """Return the Zarr path that ``file_key`` materialises to under ``target_root``.

    Strips ``.gz`` and ``.grib2`` suffixes (in that order) before appending
    ``.zarr`` so the layout mirrors the source S3 key. The trailing
    ``.zarr`` is appended to the *name* rather than swapped in via
    :meth:`Path.with_suffix`, because MRMS filenames carry product-level
    dots (``MRMS_..._00.50_…``) that Path would otherwise mistake for a
    suffix and chop off.
    """
    base = Path(target_root)
    rel = Path(file_key)
    if rel.suffix == ".gz":
        rel = rel.with_suffix("")
    if rel.suffix == ".grib2":
        rel = rel.with_suffix("")
    return base / rel.parent / (rel.name + ".zarr")


def write_dataarray_to_zarr(
    da: xr.DataArray,
    *,
    target_root: str | Path,
    file_key: str,
) -> MrmsGridLocator:
    """Write ``da`` to a Zarr store and return its :class:`MrmsGridLocator`.

    The Zarr path is derived from ``file_key`` via :func:`zarr_path_for`;
    intermediate directories are created. Existing stores at the target
    are overwritten — re-materialising an updated source file should
    produce a fresh Zarr.
    """
    target_path = zarr_path_for(target_root, file_key)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    variable = str(da.name) if da.name else "value"
    # ``DataArray.to_zarr`` requires a name; ensure one is set.
    da_named = da if da.name else da.rename(variable)
    da_named.to_zarr(str(target_path), mode="w")

    locator = MrmsGridLocator(
        file_key=file_key,
        zarr_uri=str(target_path),
        variable=variable,
        dims=tuple(str(d) for d in da_named.dims),
        shape=tuple(int(s) for s in da_named.shape),
        dtype=str(da_named.dtype),
        nbytes=int(da_named.nbytes),
    )
    log.info(
        "mrms.zarr.write",
        file_key=file_key,
        zarr_uri=locator.zarr_uri,
        variable=variable,
        shape=locator.shape,
        nbytes=locator.nbytes,
    )
    return locator


def locator_to_row_dict(locator: MrmsGridLocator) -> dict[str, Any]:
    """Convert a locator to the column dict used by the persistence layer.

    Lives next to the writer rather than the store so the on-disk shape
    of ``dims`` / ``shape`` (JSONB-serialised) is decided in one place.
    """
    return {
        "file_key": locator.file_key,
        "zarr_uri": locator.zarr_uri,
        "variable": locator.variable,
        "dims_json": json.dumps(list(locator.dims)),
        "shape_json": json.dumps(list(locator.shape)),
        "dtype": locator.dtype,
        "nbytes": locator.nbytes,
    }
