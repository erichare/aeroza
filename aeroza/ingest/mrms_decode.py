"""Download + decode for MRMS GRIB2 files.

Two pure-ish primitives that compose with the existing Zarr writer
(:mod:`aeroza.ingest.mrms_zarr`) and storage (:mod:`aeroza.ingest.mrms_grids_store`):

- :func:`download_grib2_payload` fetches an MRMS object from S3 and
  decompresses the ``.gz`` wrapper if present, returning raw GRIB2 bytes.
- :func:`decode_grib2_to_dataarray` decodes those bytes via xarray's
  ``cfgrib`` engine and returns one :class:`xarray.DataArray`.

Why split them? The download is sync I/O (boto3) but stupid simple; the
decode is CPU-bound and relies on the eccodes system library (via
:mod:`cfgrib`). Keeping them as separate small functions makes the
asyncio boundary obvious — both run on a worker thread via
``asyncio.to_thread`` from the orchestrator.

Eccodes setup (one-time, per machine):
    macOS:    ``brew install eccodes``
    Debian:   ``apt-get install libeccodes-dev``
The ``[grib]`` Python extra (``uv sync --extra grib``) installs cfgrib,
which loads ``libeccodes`` via ctypes at first decode call. Tests in this
repo mock ``xr.open_dataset`` at the boundary so the unit suite stays
green without eccodes installed.
"""

from __future__ import annotations

import gzip
import io
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from aeroza.ingest.mrms import MRMS_BUCKET

if TYPE_CHECKING:  # pragma: no cover - typing only
    import xarray as xr

log = structlog.get_logger(__name__)

CFGRIB_ENGINE: str = "cfgrib"


class MrmsDecodeError(RuntimeError):
    """Raised when a GRIB2 download or decode fails fatally."""


def download_grib2_payload(
    s3_client: Any,
    *,
    key: str,
    bucket: str = MRMS_BUCKET,
) -> bytes:
    """Fetch an MRMS object from S3 and return raw GRIB2 bytes.

    Decompresses the gzip wrapper transparently when ``key`` ends in
    ``.gz``. Synchronous on purpose — boto3 is sync, and callers run
    this inside ``asyncio.to_thread``.
    """
    log.debug("mrms.download.start", bucket=bucket, key=key)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body: bytes = bytes(response["Body"].read())
    except Exception as exc:
        raise MrmsDecodeError(f"failed to download s3://{bucket}/{key}: {exc}") from exc

    if key.endswith(".gz"):
        try:
            body = gzip.decompress(body)
        except OSError as exc:
            raise MrmsDecodeError(f"failed to gunzip s3://{bucket}/{key}: {exc}") from exc

    log.debug("mrms.download.done", bucket=bucket, key=key, bytes=len(body))
    return body


def decode_grib2_to_dataarray(
    payload: bytes,
    *,
    variable_filter: str | None = None,
) -> xr.DataArray:
    """Decode a raw GRIB2 payload into a single :class:`xarray.DataArray`.

    cfgrib reads from a file path (no in-memory buffer support), so we
    spool the payload through a tempfile. The ``.idx`` sidecar that
    cfgrib writes next to the GRIB2 file is created in the same temp
    directory and cleaned up automatically.

    ``variable_filter`` selects one variable when the GRIB2 file carries
    several; ``None`` picks the first (which is the case for every
    MRMS-published product we currently consume).
    """
    import xarray as xr

    with tempfile.TemporaryDirectory(prefix="mrms-decode-") as tmpdir:
        tmp_path = Path(tmpdir) / "payload.grib2"
        tmp_path.write_bytes(payload)
        try:
            ds = xr.open_dataset(str(tmp_path), engine=CFGRIB_ENGINE)
        except Exception as exc:
            raise MrmsDecodeError(f"cfgrib failed to open GRIB2 payload: {exc}") from exc

        variables = [str(v) for v in ds.data_vars]
        if not variables:
            raise MrmsDecodeError("GRIB2 payload contained no data variables")

        chosen: str
        if variable_filter is not None:
            if variable_filter not in variables:
                raise MrmsDecodeError(
                    f"variable {variable_filter!r} not in payload (have: {variables})"
                )
            chosen = variable_filter
        else:
            chosen = variables[0]
            if len(variables) > 1:
                log.info(
                    "mrms.decode.multi_var",
                    chose=chosen,
                    available=variables,
                )

        # Materialise the array eagerly: the tempfile is about to vanish.
        da = ds[chosen].load()
        log.debug(
            "mrms.decode.done",
            variable=chosen,
            shape=tuple(int(s) for s in da.shape),
            dtype=str(da.dtype),
        )
        return da


def gzip_payload(payload: bytes) -> bytes:
    """Helper used by tests that build synthetic ``.gz`` bodies for the
    download stub. Lives in this module so tests can import a single
    well-known compress/decompress pair.
    """
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        gz.write(payload)
    return buffer.getvalue()
