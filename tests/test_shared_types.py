"""Unit tests for shared geospatial / temporal value objects."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from aeroza.shared.types import BoundingBox, Coordinate, TimeWindow


@pytest.mark.unit
class TestCoordinate:
    def test_accepts_valid_lat_lng(self) -> None:
        c = Coordinate(lat=37.7749, lng=-122.4194)
        assert c.lat == 37.7749
        assert c.lng == -122.4194

    def test_is_frozen(self) -> None:
        c = Coordinate(lat=0.0, lng=0.0)
        with pytest.raises(AttributeError):
            c.lat = 1.0  # type: ignore[misc]

    def test_is_hashable(self) -> None:
        a = Coordinate(lat=1.0, lng=2.0)
        b = Coordinate(lat=1.0, lng=2.0)
        assert hash(a) == hash(b)
        assert {a, b} == {a}

    @pytest.mark.parametrize("lat", [-91.0, 90.0001, 1000.0])
    def test_rejects_out_of_range_latitude(self, lat: float) -> None:
        with pytest.raises(ValueError, match="latitude"):
            Coordinate(lat=lat, lng=0.0)

    @pytest.mark.parametrize("lng", [-180.0001, 181.0, 1000.0])
    def test_rejects_out_of_range_longitude(self, lng: float) -> None:
        with pytest.raises(ValueError, match="longitude"):
            Coordinate(lat=0.0, lng=lng)


@pytest.mark.unit
class TestBoundingBox:
    def test_contains_interior_point(self) -> None:
        bb = BoundingBox(min_lat=30.0, min_lng=-100.0, max_lat=40.0, max_lng=-90.0)
        assert bb.contains(Coordinate(lat=35.0, lng=-95.0))

    def test_excludes_exterior_point(self) -> None:
        bb = BoundingBox(min_lat=30.0, min_lng=-100.0, max_lat=40.0, max_lng=-90.0)
        assert not bb.contains(Coordinate(lat=29.9, lng=-95.0))

    def test_from_corners(self) -> None:
        sw = Coordinate(lat=30.0, lng=-100.0)
        ne = Coordinate(lat=40.0, lng=-90.0)
        bb = BoundingBox.from_corners(sw, ne)
        assert bb.min_lat == 30.0 and bb.max_lng == -90.0

    def test_rejects_inverted_lat(self) -> None:
        with pytest.raises(ValueError, match="min_lat"):
            BoundingBox(min_lat=40.0, min_lng=-100.0, max_lat=30.0, max_lng=-90.0)

    def test_rejects_antimeridian_crossing(self) -> None:
        with pytest.raises(ValueError, match="antimeridian"):
            BoundingBox(min_lat=0.0, min_lng=170.0, max_lat=10.0, max_lng=-170.0)


@pytest.mark.unit
class TestTimeWindow:
    def test_contains_instant_in_range(self) -> None:
        start = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        end = start + timedelta(hours=1)
        window = TimeWindow(start=start, end=end)
        assert window.contains(start + timedelta(minutes=30))

    def test_is_half_open(self) -> None:
        start = datetime(2026, 5, 1, tzinfo=UTC)
        end = start + timedelta(hours=1)
        window = TimeWindow(start=start, end=end)
        assert window.contains(start)
        assert not window.contains(end)

    def test_rejects_naive_bounds(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            TimeWindow(
                start=datetime(2026, 5, 1, 12, 0),
                end=datetime(2026, 5, 1, 13, 0),
            )

    def test_rejects_inverted_window(self) -> None:
        start = datetime(2026, 5, 1, 13, 0, tzinfo=UTC)
        end = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        with pytest.raises(ValueError, match="must precede"):
            TimeWindow(start=start, end=end)

    def test_of_helper_assumes_utc_for_naive(self) -> None:
        window = TimeWindow.of(
            start=datetime(2026, 5, 1, 12, 0),
            end=datetime(2026, 5, 1, 13, 0),
        )
        assert window.start.tzinfo is UTC
