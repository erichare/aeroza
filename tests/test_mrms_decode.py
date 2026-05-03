"""Unit tests for the MRMS GRIB2 download + decode primitives.

Both the S3 client and ``xr.open_dataset`` are stubbed, so this suite
runs without ``eccodes`` installed and without network access. The real
cfgrib path is exercised by an end-to-end smoke that's deferred to a
follow-up CI job (eccodes install + bundled fixture).
"""

from __future__ import annotations

import io
from typing import Any
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

from aeroza.ingest.mrms_decode import (
    CFGRIB_INSTALL_HINT,
    CfgribUnavailableError,
    MrmsDecodeError,
    decode_grib2_to_dataarray,
    download_grib2_payload,
    ensure_cfgrib_available,
    gzip_payload,
)

pytestmark = pytest.mark.unit

# --------------------------------------------------------------------------- #
# Stub S3 client                                                               #
# --------------------------------------------------------------------------- #


class _StubBody:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body


class StubS3Client:
    """Records every ``get_object`` call and returns canned bodies by key."""

    def __init__(self, bodies: dict[str, bytes] | None = None) -> None:
        self.bodies = bodies or {}
        self.calls: list[dict[str, str]] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.calls.append({"Bucket": Bucket, "Key": Key})
        if Key not in self.bodies:
            raise KeyError(f"unknown stub key {Key!r}")
        return {"Body": _StubBody(self.bodies[Key])}


# --------------------------------------------------------------------------- #
# download_grib2_payload                                                       #
# --------------------------------------------------------------------------- #


class TestDownloadGrib2Payload:
    def test_returns_raw_bytes_for_uncompressed_key(self) -> None:
        client = StubS3Client(bodies={"x.grib2": b"raw-grib2-bytes"})
        payload = download_grib2_payload(client, key="x.grib2", bucket="bkt")
        assert payload == b"raw-grib2-bytes"
        assert client.calls == [{"Bucket": "bkt", "Key": "x.grib2"}]

    def test_decompresses_gzip_when_key_ends_with_gz(self) -> None:
        original = b"the actual GRIB2 payload"
        compressed = gzip_payload(original)
        # Sanity: compressed and uncompressed differ.
        assert compressed != original
        client = StubS3Client(bodies={"x.grib2.gz": compressed})
        payload = download_grib2_payload(client, key="x.grib2.gz", bucket="bkt")
        assert payload == original

    def test_uses_default_bucket_when_unspecified(self) -> None:
        client = StubS3Client(bodies={"k": b"data"})
        download_grib2_payload(client, key="k")
        assert client.calls[0]["Bucket"] == "noaa-mrms-pds"

    def test_wraps_s3_failure_in_decode_error(self) -> None:
        class FlakyClient:
            def get_object(self, **_: Any) -> dict[str, Any]:
                raise RuntimeError("403 forbidden")

        with pytest.raises(MrmsDecodeError, match="failed to download"):
            download_grib2_payload(FlakyClient(), key="x.grib2", bucket="b")

    def test_wraps_corrupt_gzip_in_decode_error(self) -> None:
        client = StubS3Client(bodies={"x.gz": b"this is definitely not gzip"})
        with pytest.raises(MrmsDecodeError, match="gunzip"):
            download_grib2_payload(client, key="x.gz", bucket="b")


# --------------------------------------------------------------------------- #
# decode_grib2_to_dataarray (xr.open_dataset patched)                          #
# --------------------------------------------------------------------------- #


def _synthetic_dataset(*, vars: tuple[str, ...] = ("reflectivity",)) -> xr.Dataset:
    """Build a tiny in-memory xarray.Dataset to stand in for cfgrib output."""
    arrays = {
        name: xr.DataArray(
            np.arange(20, dtype=np.float32).reshape(4, 5),
            dims=("latitude", "longitude"),
            name=name,
        )
        for name in vars
    }
    return xr.Dataset(arrays)


class TestDecodeGrib2ToDataarray:
    def test_returns_first_data_var_when_no_filter(self) -> None:
        with patch("xarray.open_dataset", return_value=_synthetic_dataset()) as opener:
            da = decode_grib2_to_dataarray(b"ignored")
        assert isinstance(da, xr.DataArray)
        assert da.name == "reflectivity"
        assert da.shape == (4, 5)
        # Engine is hard-coded to cfgrib so consumers don't have to think about it.
        assert opener.call_args.kwargs["engine"] == "cfgrib"

    def test_picks_named_variable_when_filter_set(self) -> None:
        ds = _synthetic_dataset(vars=("a", "b"))
        with patch("xarray.open_dataset", return_value=ds):
            da = decode_grib2_to_dataarray(b"ignored", variable_filter="b")
        assert da.name == "b"

    def test_raises_when_filter_not_present(self) -> None:
        ds = _synthetic_dataset(vars=("a", "b"))
        with (
            patch("xarray.open_dataset", return_value=ds),
            pytest.raises(MrmsDecodeError, match="not in payload"),
        ):
            decode_grib2_to_dataarray(b"ignored", variable_filter="missing")

    def test_raises_when_payload_has_no_variables(self) -> None:
        with (
            patch("xarray.open_dataset", return_value=xr.Dataset()),
            pytest.raises(MrmsDecodeError, match="no data variables"),
        ):
            decode_grib2_to_dataarray(b"ignored")

    def test_wraps_cfgrib_failure_in_decode_error(self) -> None:
        with (
            patch("xarray.open_dataset", side_effect=RuntimeError("eccodes load failure")),
            pytest.raises(MrmsDecodeError, match="cfgrib failed to open"),
        ):
            decode_grib2_to_dataarray(b"ignored")

    def test_cfgrib_not_registered_surfaces_install_hint(self) -> None:
        """xarray's 'unrecognized engine' ValueError gets re-wrapped as
        a CfgribUnavailableError carrying the install hint, not the
        cryptic original. Subclass relationship preserved so callers
        catching MrmsDecodeError still work."""
        # Reproduce xarray's exact wording so the substring detection
        # in mrms_decode keeps tracking it.
        xarray_msg = (
            "unrecognized engine 'cfgrib' must be one of your download engines: ['store', 'zarr']"
        )
        with (
            patch("xarray.open_dataset", side_effect=ValueError(xarray_msg)),
            pytest.raises(CfgribUnavailableError, match="brew install eccodes"),
        ):
            decode_grib2_to_dataarray(b"ignored")
        # Subclass relationship preserved so existing handlers still catch.
        assert issubclass(CfgribUnavailableError, MrmsDecodeError)


class TestEnsureCfgribAvailable:
    def test_raises_when_cfgrib_engine_is_not_registered(self) -> None:
        # xarray.backends.plugins.get_backend is what we probe; stub it
        # to mimic the "engine not installed" case.
        with (
            patch(
                "xarray.backends.plugins.get_backend",
                side_effect=ValueError("unrecognized engine 'cfgrib'"),
            ),
            pytest.raises(CfgribUnavailableError, match="uv sync --extra grib"),
        ):
            ensure_cfgrib_available()

    def test_passes_when_cfgrib_engine_is_registered(self) -> None:
        # A truthy return value is enough — we don't actually use the
        # backend object, we just check the lookup didn't raise.
        with patch("xarray.backends.plugins.get_backend", return_value=object()):
            ensure_cfgrib_available()  # must not raise

    def test_install_hint_mentions_both_install_paths(self) -> None:
        # Sanity-check the hint covers macOS + Linux + the python extra
        # so the operator can identify their path without scrolling docs.
        assert "brew install eccodes" in CFGRIB_INSTALL_HINT
        assert "apt-get install" in CFGRIB_INSTALL_HINT
        assert "uv sync --extra grib" in CFGRIB_INSTALL_HINT

    def test_writes_payload_to_tempfile_so_cfgrib_can_read_a_path(self) -> None:
        captured: dict[str, Any] = {}

        def fake_open(path: str, **kwargs: Any) -> xr.Dataset:
            captured["path"] = path
            captured["kwargs"] = kwargs
            return _synthetic_dataset()

        with patch("xarray.open_dataset", side_effect=fake_open):
            decode_grib2_to_dataarray(b"some-grib2-bytes")

        # The function spools the payload through a tempfile and passes that
        # path to xr.open_dataset; the path should reference an existing-on-disk
        # location at call time (cfgrib needs a real file, not bytes).
        assert "payload.grib2" in captured["path"]
        assert captured["kwargs"]["engine"] == "cfgrib"


# --------------------------------------------------------------------------- #
# gzip_payload helper                                                          #
# --------------------------------------------------------------------------- #


def test_gzip_payload_round_trips_via_gzip_module() -> None:
    import gzip

    original = b"this is the payload"
    blob = gzip_payload(original)
    assert gzip.decompress(blob) == original
    # Also verify it's a real gzip header (1f 8b magic).
    assert blob[:2] == b"\x1f\x8b"


def test_gzip_payload_returns_distinct_bytes_for_distinct_input() -> None:
    a = gzip_payload(b"x")
    b = gzip_payload(b"y")
    assert a != b


def test_gzip_helper_uses_bytesio_under_the_hood() -> None:
    """Smoke that the helper exposes the BytesIO surface we expect — the
    test isn't strictly necessary, but it pins the helper to a contract
    that other suites (orchestrator integration) rely on."""
    blob = gzip_payload(b"abc")
    assert isinstance(blob, bytes)
    # It should be readable as a gzip stream via the standard library.
    import gzip

    with gzip.GzipFile(fileobj=io.BytesIO(blob), mode="rb") as gz:
        assert gz.read() == b"abc"
