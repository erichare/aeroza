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


class CfgribUnavailableError(MrmsDecodeError):
    """Raised when xarray can't load the cfgrib engine.

    Distinct from generic decode failures so callers (notably the
    materialiser worker) can surface the install hint directly instead
    of forwarding a cryptic xarray ValueError. The two paths to this
    error: the ``[grib]`` Python extra wasn't installed, OR cfgrib is
    installed but the system ``libeccodes`` it loads via ctypes is
    missing.
    """


# How to detect xarray's "engine not registered" error without depending on
# its concrete exception type (it raises a plain ValueError). Substring
# match is brittle but xarray's wording has been stable across 2023→
# releases, and the test in tests/test_mrms_decode.py pins the contract.
_CFGRIB_NOT_REGISTERED_NEEDLE = "unrecognized engine 'cfgrib'"

# Operator-facing install hint. Centralised so the worker startup probe
# and the per-file decode error use identical wording.
CFGRIB_INSTALL_HINT: str = (
    "cfgrib isn't available — the GRIB2 decoder needs both the [grib] "
    "Python extra and the eccodes system library:\n"
    "  macOS:  brew install eccodes && uv sync --extra grib\n"
    "  Linux:  sudo apt-get install -y libeccodes-dev && uv sync --extra grib\n"
    "Then restart `aeroza-materialise-mrms`."
)


def ensure_cfgrib_available() -> None:
    """Probe whether xarray can load the cfgrib engine; raise on failure.

    Designed for the materialiser worker's startup path so it fails fast
    with a clear install hint instead of grinding through every queued
    grid with the same per-file error. Cheap (one xarray plugin lookup,
    no actual GRIB decode), idempotent, side-effect-free.
    """
    try:
        from xarray.backends import plugins

        plugins.get_backend(CFGRIB_ENGINE)
    except Exception as exc:
        raise CfgribUnavailableError(CFGRIB_INSTALL_HINT) from exc


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
            # Distinguish "cfgrib isn't installed" from "cfgrib is here
            # but couldn't decode this specific file". Same install hint
            # the worker startup probe uses, so the message is identical
            # whichever path surfaces it.
            if _CFGRIB_NOT_REGISTERED_NEEDLE in str(exc):
                raise CfgribUnavailableError(CFGRIB_INSTALL_HINT) from exc
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
