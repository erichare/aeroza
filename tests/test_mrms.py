"""Unit tests for MRMS discovery primitives."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from aeroza.ingest.mrms import (
    MRMS_BUCKET,
    MrmsFile,
    list_mrms_files,
    parse_mrms_key,
)


@pytest.mark.unit
class TestParseMrmsKey:
    def test_parses_canonical_reflectivity_key(self) -> None:
        key = (
            "CONUS/MergedReflectivityComposite_00.50/20260501/"
            "MRMS_MergedReflectivityComposite_00.50_20260501-122000.grib2.gz"
        )
        result = parse_mrms_key(key)
        assert result is not None
        assert result.product == "MergedReflectivityComposite"
        assert result.level == "00.50"
        assert result.valid_at == datetime(2026, 5, 1, 12, 20, 0, tzinfo=UTC)
        assert result.product_level == "MergedReflectivityComposite_00.50"

    def test_parses_uncompressed_grib2_key(self) -> None:
        key = "CONUS/PrecipRate_00.00/20260501/MRMS_PrecipRate_00.00_20260501-001500.grib2"
        result = parse_mrms_key(key)
        assert result is not None
        assert result.product == "PrecipRate"
        assert result.level == "00.00"
        assert result.valid_at == datetime(2026, 5, 1, 0, 15, 0, tzinfo=UTC)

    @pytest.mark.parametrize(
        "key",
        [
            "",
            "CONUS/",
            "CONUS/MergedReflectivityComposite_00.50/",
            "CONUS/MergedReflectivityComposite_00.50/20260501/",
            "OTHER/MergedReflectivityComposite_00.50/20260501/MRMS_X.grib2.gz",
            # mismatched product token between prefix and filename
            "CONUS/MergedReflectivityComposite_00.50/20260501/MRMS_PrecipRate_00.00_20260501-122000.grib2.gz",
            # wrong suffix
            "CONUS/MergedReflectivityComposite_00.50/20260501/MRMS_MergedReflectivityComposite_00.50_20260501-122000.netcdf",
            # bad timestamp
            "CONUS/MergedReflectivityComposite_00.50/20260501/MRMS_MergedReflectivityComposite_00.50_20269999-122000.grib2.gz",
        ],
    )
    def test_returns_none_for_unparseable_key(self, key: str) -> None:
        assert parse_mrms_key(key) is None

    def test_default_size_and_etag_are_unset(self) -> None:
        key = (
            "CONUS/MergedReflectivityComposite_00.50/20260501/"
            "MRMS_MergedReflectivityComposite_00.50_20260501-122000.grib2.gz"
        )
        parsed = parse_mrms_key(key)
        assert parsed is not None
        assert parsed.size_bytes == 0
        assert parsed.etag is None

    def test_valid_at_is_timezone_aware_utc(self) -> None:
        key = (
            "CONUS/MergedReflectivityComposite_00.50/20260501/"
            "MRMS_MergedReflectivityComposite_00.50_20260501-122000.grib2.gz"
        )
        parsed = parse_mrms_key(key)
        assert parsed is not None
        assert parsed.valid_at.tzinfo is UTC


class _StubPaginator:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self._pages = pages
        self.captured_calls: list[tuple[str, str]] = []

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, Any]]:
        self.captured_calls.append((Bucket, Prefix))
        return [{"Contents": page} for page in self._pages]


class _StubS3Client:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.paginator = _StubPaginator(pages)

    def get_paginator(self, _name: str) -> _StubPaginator:
        return self.paginator


def _obj(
    key: str, size: int = 1024, etag: str = '"abc123"', last_modified: datetime | None = None
) -> dict[str, Any]:
    obj = {"Key": key, "Size": size, "ETag": etag}
    if last_modified is not None:
        obj["LastModified"] = last_modified
    return obj


@pytest.mark.unit
class TestListMrmsFiles:
    async def test_lists_and_parses_files(self) -> None:
        client = _StubS3Client(
            pages=[
                [
                    _obj(
                        "CONUS/MergedReflectivityComposite_00.50/20260501/"
                        "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz",
                        size=10_000,
                        etag='"deadbeef"',
                    ),
                    _obj(
                        "CONUS/MergedReflectivityComposite_00.50/20260501/"
                        "MRMS_MergedReflectivityComposite_00.50_20260501-120200.grib2.gz",
                        size=11_000,
                        etag='"feedface"',
                    ),
                ]
            ]
        )
        files = await list_mrms_files(
            product="MergedReflectivityComposite",
            level="00.50",
            day=datetime(2026, 5, 1, tzinfo=UTC),
            s3_client=client,
        )
        assert len(files) == 2
        assert client.paginator.captured_calls == [
            (MRMS_BUCKET, "CONUS/MergedReflectivityComposite_00.50/20260501/")
        ]
        # Returned in chronological order; sizes and ETags propagate.
        assert files[0].valid_at == datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        assert files[0].size_bytes == 10_000
        assert files[0].etag == "deadbeef"
        assert files[1].valid_at == datetime(2026, 5, 1, 12, 2, 0, tzinfo=UTC)
        assert files[1].etag == "feedface"

    async def test_skips_unparseable_keys(self) -> None:
        client = _StubS3Client(
            pages=[
                [
                    _obj("CONUS/MergedReflectivityComposite_00.50/20260501/"),  # placeholder
                    _obj(
                        "CONUS/MergedReflectivityComposite_00.50/20260501/"
                        "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz"
                    ),
                ]
            ]
        )
        files = await list_mrms_files(
            product="MergedReflectivityComposite",
            level="00.50",
            day=datetime(2026, 5, 1, tzinfo=UTC),
            s3_client=client,
        )
        assert len(files) == 1
        assert files[0].valid_at == datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)

    async def test_filters_by_since_and_until(self) -> None:
        client = _StubS3Client(
            pages=[
                [
                    _obj(
                        "CONUS/PrecipRate_00.00/20260501/"
                        "MRMS_PrecipRate_00.00_20260501-115800.grib2.gz"
                    ),
                    _obj(
                        "CONUS/PrecipRate_00.00/20260501/"
                        "MRMS_PrecipRate_00.00_20260501-120000.grib2.gz"
                    ),
                    _obj(
                        "CONUS/PrecipRate_00.00/20260501/"
                        "MRMS_PrecipRate_00.00_20260501-120200.grib2.gz"
                    ),
                    _obj(
                        "CONUS/PrecipRate_00.00/20260501/"
                        "MRMS_PrecipRate_00.00_20260501-120400.grib2.gz"
                    ),
                ]
            ]
        )
        files = await list_mrms_files(
            product="PrecipRate",
            level="00.00",
            day=datetime(2026, 5, 1, tzinfo=UTC),
            since=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
            until=datetime(2026, 5, 1, 12, 4, tzinfo=UTC),
            s3_client=client,
        )
        assert [f.valid_at.minute * 60 + f.valid_at.second for f in files] == [0, 120]

    async def test_paginates_across_pages(self) -> None:
        client = _StubS3Client(
            pages=[
                [
                    _obj(
                        "CONUS/PrecipRate_00.00/20260501/"
                        "MRMS_PrecipRate_00.00_20260501-120000.grib2.gz"
                    )
                ],
                [
                    _obj(
                        "CONUS/PrecipRate_00.00/20260501/"
                        "MRMS_PrecipRate_00.00_20260501-120200.grib2.gz"
                    )
                ],
                [
                    _obj(
                        "CONUS/PrecipRate_00.00/20260501/"
                        "MRMS_PrecipRate_00.00_20260501-120400.grib2.gz"
                    )
                ],
            ]
        )
        files = await list_mrms_files(
            product="PrecipRate",
            level="00.00",
            day=datetime(2026, 5, 1, tzinfo=UTC),
            s3_client=client,
        )
        assert len(files) == 3

    async def test_omitting_day_uses_product_root_prefix(self) -> None:
        client = _StubS3Client(pages=[[]])
        await list_mrms_files(
            product="PrecipRate",
            level="00.00",
            s3_client=client,
        )
        assert client.paginator.captured_calls == [(MRMS_BUCKET, "CONUS/PrecipRate_00.00/")]

    async def test_returns_empty_for_empty_bucket_listing(self) -> None:
        client = _StubS3Client(pages=[[]])
        files = await list_mrms_files(
            product="PrecipRate",
            level="00.00",
            day=datetime(2026, 5, 1, tzinfo=UTC),
            s3_client=client,
        )
        assert files == ()

    async def test_overrides_bucket_for_tests(self) -> None:
        client = _StubS3Client(pages=[[]])
        await list_mrms_files(
            product="PrecipRate",
            level="00.00",
            day=datetime(2026, 5, 1, tzinfo=UTC),
            s3_client=client,
            bucket="my-test-bucket",
        )
        assert client.paginator.captured_calls == [
            ("my-test-bucket", "CONUS/PrecipRate_00.00/20260501/")
        ]


@pytest.mark.unit
def test_mrms_file_is_frozen_and_hashable() -> None:
    a = MrmsFile(
        key="x",
        product="P",
        level="00.00",
        valid_at=datetime(2026, 5, 1, tzinfo=UTC),
        size_bytes=0,
        etag=None,
    )
    b = MrmsFile(
        key="x",
        product="P",
        level="00.00",
        valid_at=datetime(2026, 5, 1, tzinfo=UTC),
        size_bytes=0,
        etag=None,
    )
    assert hash(a) == hash(b)
    assert {a, b} == {a}
