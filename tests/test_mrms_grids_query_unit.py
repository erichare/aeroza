"""Unit tests for the materialised-grid query layer (no DB).

Covers the JSONB row-shape parsing and the view → wire-item mapping.
The end-to-end repo + route tests live in ``test_v1_mrms_grids.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from aeroza.query.mrms_grids import (
    MrmsGridView,
    _parse_jsonb_ints,
    _parse_jsonb_strings,
    mrms_grid_view_to_item,
)

pytestmark = pytest.mark.unit


def _view(**overrides: object) -> MrmsGridView:
    base: dict[str, object] = {
        "file_key": "CONUS/.../MRMS_..._120000.grib2.gz",
        "product": "MergedReflectivityComposite",
        "level": "00.50",
        "valid_at": datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        "zarr_uri": "/var/data/mrms.zarr",
        "variable": "reflectivity",
        "dims": ("latitude", "longitude"),
        "shape": (3500, 7000),
        "dtype": "float32",
        "nbytes": 3500 * 7000 * 4,
        "materialised_at": datetime(2026, 5, 1, 12, 1, tzinfo=UTC),
    }
    base.update(overrides)
    return MrmsGridView(**base)  # type: ignore[arg-type]


class TestParseJsonbStrings:
    def test_passes_through_python_list(self) -> None:
        assert _parse_jsonb_strings(["lat", "lon"]) == ("lat", "lon")

    def test_decodes_json_string(self) -> None:
        assert _parse_jsonb_strings('["lat","lon"]') == ("lat", "lon")

    def test_coerces_non_str_elements(self) -> None:
        # Defensive — JSONB rarely returns non-strings here, but better to coerce
        # than to surface an asymmetric shape.
        assert _parse_jsonb_strings([1, 2]) == ("1", "2")


class TestParseJsonbInts:
    def test_passes_through_python_list(self) -> None:
        assert _parse_jsonb_ints([3500, 7000]) == (3500, 7000)

    def test_decodes_json_string(self) -> None:
        assert _parse_jsonb_ints("[3500,7000]") == (3500, 7000)

    def test_coerces_floatish_strings(self) -> None:
        # JSONB never produces this, but the helper should be robust.
        assert _parse_jsonb_ints(["3500", "7000"]) == (3500, 7000)


class TestViewToItem:
    def test_field_renaming_uses_camelcase_aliases(self) -> None:
        item = mrms_grid_view_to_item(_view())
        wire = item.model_dump(by_alias=True, mode="json")
        assert wire["fileKey"] == "CONUS/.../MRMS_..._120000.grib2.gz"
        assert wire["zarrUri"] == "/var/data/mrms.zarr"
        assert wire["validAt"] == "2026-05-01T12:00:00Z"
        assert wire["materialisedAt"] == "2026-05-01T12:01:00Z"
        # Underscored keys must NOT leak through.
        assert "file_key" not in wire
        assert "zarr_uri" not in wire
        assert "valid_at" not in wire
        assert "materialised_at" not in wire

    def test_dims_and_shape_round_trip(self) -> None:
        item = mrms_grid_view_to_item(_view(dims=("time", "y", "x"), shape=(1, 256, 256)))
        wire = item.model_dump(by_alias=True, mode="json")
        assert wire["dims"] == ["time", "y", "x"]
        assert wire["shape"] == [1, 256, 256]

    def test_preserves_locator_metadata(self) -> None:
        item = mrms_grid_view_to_item(_view())
        wire = item.model_dump(by_alias=True, mode="json")
        assert wire["variable"] == "reflectivity"
        assert wire["dtype"] == "float32"
        assert wire["nbytes"] == 3500 * 7000 * 4
        assert wire["product"] == "MergedReflectivityComposite"
        assert wire["level"] == "00.50"
