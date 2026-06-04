"""Unit tests for the Cloudflare R2 client.

The whole point of this module is "the right calls go through to S3
under the right keys with the right headers". boto3 is the hard part
(networked, configured, side-effecting), so we mock it at the
``self._client`` layer and assert against the recorded call shape —
exactly the layer the rest of the codebase interacts with.

No real R2 traffic, no environment dependency, runs as a unit test.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from aeroza.config import Settings
from aeroza.tiles.r2 import (
    LATEST_POINTER_KEY,
    POINTER_CACHE_CONTROL,
    TILE_CACHE_CONTROL,
    R2Client,
    build_r2_client,
)

pytestmark = pytest.mark.unit


def _fake_client(*, head_status: int | None = None) -> MagicMock:
    """Return a MagicMock shaped like a boto3 S3 client.

    ``head_status`` controls how the mock's ``head_object`` behaves:
    pass ``404`` to simulate a missing key (raises ClientError),
    ``None`` to simulate an existing key (returns a stub dict).
    """
    client = MagicMock()
    # ``client.exceptions.ClientError`` is the path the R2Client uses
    # to catch missing-key responses. Mirror it onto our mock so
    # ``except self._client.exceptions.ClientError`` matches.
    client.exceptions = MagicMock()
    client.exceptions.ClientError = ClientError

    if head_status == 404:
        error_response: dict[str, Any] = {
            "Error": {"Code": "404", "Message": "Not Found"},
        }
        client.head_object.side_effect = ClientError(error_response, "HeadObject")
    else:
        client.head_object.return_value = {"ContentLength": 1024}

    client.put_object.return_value = {"ETag": '"deadbeef"'}
    return client


def _client_with_pages(pages: list[dict[str, Any]]) -> MagicMock:
    """Build a mock that returns the supplied list of ``list_objects_v2``
    page payloads in order. Caller is responsible for setting the
    ``IsTruncated`` / ``NextContinuationToken`` keys appropriately.
    """
    client = MagicMock()
    client.exceptions = MagicMock()
    client.exceptions.ClientError = ClientError
    client.list_objects_v2.side_effect = pages
    client.delete_objects.return_value = {}
    return client


async def test_put_tile_writes_to_expected_key_with_cache_headers() -> None:
    """One ``put_tile`` call → one ``put_object`` with the right key,
    Content-Type, and forever-immutable Cache-Control. That last
    header is what makes the Cloudflare edge serve the tile back at
    sub-100ms for the lifetime of the bucket — getting it wrong is
    silent and ruinous to UX, so pin it in a test.
    """
    fake = _fake_client()
    r2 = R2Client(bucket="bk", endpoint="https://e.example", _client=fake)

    await r2.put_tile(
        file_key="CONUS/MergedReflectivityComposite_00.50/path/to/grid.grib2.gz",
        z=5,
        x=12,
        y=8,
        fmt="webp",
        body=b"\x00\x01\x02\x03",
    )

    fake.put_object.assert_called_once()
    kwargs = fake.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "bk"
    # Key shape: ``{file_key}/{z}/{x}/{y}.{format}``. Slashes inside
    # the fileKey are preserved as path separators — R2 treats keys
    # as opaque strings, and the frontend builds the URL the same way.
    assert kwargs["Key"] == (
        "CONUS/MergedReflectivityComposite_00.50/path/to/grid.grib2.gz/5/12/8.webp"
    )
    assert kwargs["ContentType"] == "image/webp"
    assert kwargs["CacheControl"] == TILE_CACHE_CONTROL
    assert kwargs["Body"] == b"\x00\x01\x02\x03"


async def test_put_tile_supports_png_format() -> None:
    """PNG keys use the ``.png`` extension and ``image/png`` MIME —
    same key shape as WebP otherwise. The prewarm path is WebP-only,
    but the helper supports both for completeness / future PNG-only
    callers (curl-style smoke tests, e.g.)."""
    fake = _fake_client()
    r2 = R2Client(bucket="bk", endpoint="https://e", _client=fake)

    await r2.put_tile(file_key="fk", z=3, x=1, y=2, fmt="png", body=b"abc")

    kwargs = fake.put_object.call_args.kwargs
    assert kwargs["Key"] == "fk/3/1/2.png"
    assert kwargs["ContentType"] == "image/png"


async def test_put_tile_rejects_unsupported_format() -> None:
    fake = _fake_client()
    r2 = R2Client(bucket="bk", endpoint="https://e", _client=fake)

    with pytest.raises(ValueError, match="unsupported tile format"):
        await r2.put_tile(file_key="fk", z=0, x=0, y=0, fmt="jpg", body=b"")


async def test_put_latest_pointer_writes_json_with_short_cache_control() -> None:
    """The pointer is the one object that must NOT be immutable-cached: it
    flips every grid cycle. Assert the key, JSON body shape, JSON MIME, and
    the short Cache-Control — getting any of these wrong silently pins the
    map to a stale grid at the CDN edge.
    """
    fake = _fake_client()
    r2 = R2Client(bucket="bk", endpoint="https://e.example", _client=fake)

    await r2.put_latest_pointer(
        file_key="CONUS/MergedReflectivityComposite_00.50/20260604/grid.grib2.gz",
        valid_at="2026-06-04T17:20:42+00:00",
        product="MergedReflectivityComposite",
        level="00.50",
    )

    fake.put_object.assert_called_once()
    kwargs = fake.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "bk"
    assert kwargs["Key"] == LATEST_POINTER_KEY
    assert kwargs["ContentType"] == "application/json"
    assert kwargs["CacheControl"] == POINTER_CACHE_CONTROL
    assert kwargs["CacheControl"] != TILE_CACHE_CONTROL  # never immutable
    payload = json.loads(kwargs["Body"])
    assert payload == {
        "fileKey": "CONUS/MergedReflectivityComposite_00.50/20260604/grid.grib2.gz",
        "validAt": "2026-06-04T17:20:42+00:00",
        "product": "MergedReflectivityComposite",
        "level": "00.50",
    }


async def test_object_exists_returns_true_when_head_succeeds() -> None:
    fake = _fake_client(head_status=None)
    r2 = R2Client(bucket="bk", endpoint="https://e", _client=fake)

    found = await r2.object_exists(file_key="fk", z=4, x=1, y=2, fmt="webp")

    assert found is True
    fake.head_object.assert_called_once_with(Bucket="bk", Key="fk/4/1/2.webp")


async def test_object_exists_returns_false_on_404() -> None:
    """The prewarm consumer uses ``object_exists`` as a render
    short-circuit, so a missing key MUST surface as ``False`` rather
    than propagating the ClientError — otherwise NATS at-least-once
    redelivery would explode instead of skipping.
    """
    fake = _fake_client(head_status=404)
    r2 = R2Client(bucket="bk", endpoint="https://e", _client=fake)

    found = await r2.object_exists(file_key="fk", z=4, x=1, y=2, fmt="webp")

    assert found is False


async def test_object_exists_reraises_non_404_errors() -> None:
    """A 503 from R2 (rare, transient) is NOT a "missing key" — we
    want the caller to retry on the next event, not silently skip.
    Only ``404 / NoSuchKey / NotFound`` swallow."""
    fake = MagicMock()
    fake.exceptions = MagicMock()
    fake.exceptions.ClientError = ClientError
    fake.head_object.side_effect = ClientError(
        {"Error": {"Code": "503", "Message": "Slow Down"}},
        "HeadObject",
    )
    r2 = R2Client(bucket="bk", endpoint="https://e", _client=fake)

    with pytest.raises(ClientError):
        await r2.object_exists(file_key="fk", z=4, x=1, y=2, fmt="webp")


async def test_delete_grid_pages_through_results_and_returns_count() -> None:
    """``delete_grid`` must handle the multi-page case — a typical
    CONUS pyramid is just over 1000 keys when both formats land, and
    R2's ``list_objects_v2`` caps at that. Two pages exercise the
    continuation-token + pagination path explicitly.
    """
    page1 = {
        "Contents": [{"Key": f"fk/4/{i}/0.webp"} for i in range(3)],
        "IsTruncated": True,
        "NextContinuationToken": "tok-1",
    }
    page2 = {
        "Contents": [{"Key": f"fk/5/{i}/0.webp"} for i in range(2)],
        "IsTruncated": False,
    }
    fake = _client_with_pages([page1, page2])
    r2 = R2Client(bucket="bk", endpoint="https://e", _client=fake)

    removed = await r2.delete_grid(file_key="fk")

    assert removed == 5
    # First list_objects_v2 call has no token; second carries the
    # token returned by page 1.
    assert fake.list_objects_v2.call_args_list[0].kwargs == {
        "Bucket": "bk",
        "Prefix": "fk/",
        "MaxKeys": 1000,
    }
    assert fake.list_objects_v2.call_args_list[1].kwargs == {
        "Bucket": "bk",
        "Prefix": "fk/",
        "MaxKeys": 1000,
        "ContinuationToken": "tok-1",
    }
    # delete_objects fires once per non-empty page with the Quiet
    # flag so R2 doesn't echo back every key.
    assert fake.delete_objects.call_count == 2


async def test_delete_grid_returns_zero_when_prefix_is_empty() -> None:
    """A prune for a fileKey that R2 has no record of (e.g. tiles
    weren't uploaded successfully on materialise) must succeed with
    a zero count — orphan-recovery, not error."""
    page = {"Contents": [], "IsTruncated": False}
    fake = _client_with_pages([page])
    r2 = R2Client(bucket="bk", endpoint="https://e", _client=fake)

    removed = await r2.delete_grid(file_key="fk")

    assert removed == 0
    fake.delete_objects.assert_not_called()


def test_public_url_mirrors_object_key() -> None:
    r2 = R2Client(bucket="bk", endpoint="https://e", _client=MagicMock())
    url = r2.public_url(
        base_url="https://tiles.aeroza.app/",
        file_key="CONUS/foo",
        z=4,
        x=1,
        y=2,
        fmt="webp",
    )
    # Trailing slash in base_url is stripped; object key follows the
    # same shape as the R2 PUT key.
    assert url == "https://tiles.aeroza.app/CONUS/foo/4/1/2.webp"


def test_build_r2_client_returns_none_when_settings_blank() -> None:
    """Missing credentials → no client. Lets local dev (no R2 env
    configured) and the test suite operate without a Cloudflare
    account."""
    blank = Settings(
        r2_endpoint="",
        r2_bucket="",
        r2_access_key_id="",
        r2_secret_access_key="",
    )
    assert build_r2_client(blank) is None


def test_build_r2_client_constructs_when_settings_complete() -> None:
    """All four R2 settings populated → a real client. We don't make a
    network call here; just verify the factory wires through and
    binds to the right bucket name."""
    settings = Settings(
        r2_endpoint="https://example.r2.cloudflarestorage.com",
        r2_bucket="aeroza-tiles-test",
        r2_access_key_id="AKIA-test",
        r2_secret_access_key="secret-test",
    )
    client = build_r2_client(settings)
    assert client is not None
    assert client.bucket == "aeroza-tiles-test"
    assert client.endpoint == "https://example.r2.cloudflarestorage.com"
