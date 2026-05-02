"""Unit tests for query helpers — no DB required."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from aeroza.ingest.nws_alerts import Severity, severities_at_least, severity_rank
from aeroza.query.parsers import parse_bbox, parse_point, parse_polygon
from aeroza.shared.types import BoundingBox, Coordinate


@pytest.mark.unit
class TestSeverityRank:
    def test_extreme_outranks_severe(self) -> None:
        assert severity_rank(Severity.EXTREME) > severity_rank(Severity.SEVERE)

    def test_severe_outranks_moderate(self) -> None:
        assert severity_rank(Severity.SEVERE) > severity_rank(Severity.MODERATE)

    def test_unknown_is_lowest(self) -> None:
        ranks = [severity_rank(s) for s in Severity]
        assert severity_rank(Severity.UNKNOWN) == min(ranks)

    def test_unknown_string_falls_back_to_zero(self) -> None:
        assert severity_rank("CompletelyMadeUp") == 0


@pytest.mark.unit
class TestSeveritiesAtLeast:
    def test_at_least_severe_includes_extreme_and_severe(self) -> None:
        names = severities_at_least(Severity.SEVERE)
        assert set(names) == {Severity.EXTREME.value, Severity.SEVERE.value}

    def test_at_least_minor_includes_everything_above_unknown(self) -> None:
        names = severities_at_least(Severity.MINOR)
        assert set(names) == {
            Severity.EXTREME.value,
            Severity.SEVERE.value,
            Severity.MODERATE.value,
            Severity.MINOR.value,
        }

    def test_at_least_unknown_includes_everything(self) -> None:
        names = severities_at_least(Severity.UNKNOWN)
        assert set(names) == {s.value for s in Severity}

    def test_returns_immutable_tuple(self) -> None:
        assert isinstance(severities_at_least(Severity.SEVERE), tuple)


@pytest.mark.unit
class TestParsePoint:
    def test_returns_none_for_none_input(self) -> None:
        assert parse_point(None) is None

    def test_parses_well_formed_point(self) -> None:
        assert parse_point("29.76,-95.37") == Coordinate(lat=29.76, lng=-95.37)

    def test_accepts_whole_numbers(self) -> None:
        assert parse_point("0,0") == Coordinate(lat=0.0, lng=0.0)

    @pytest.mark.parametrize("raw", ["", "29.76", "29.76,-95.37,extra"])
    def test_rejects_malformed_shape(self, raw: str) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_point(raw)
        assert exc.value.status_code == 400
        assert "lat,lng" in str(exc.value.detail)

    def test_rejects_non_numeric(self) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_point("north,east")
        assert exc.value.status_code == 400
        assert "numeric" in str(exc.value.detail)

    def test_rejects_out_of_range_latitude(self) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_point("91,0")
        assert exc.value.status_code == 400
        assert "latitude" in str(exc.value.detail)

    def test_rejects_out_of_range_longitude(self) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_point("0,181")
        assert exc.value.status_code == 400
        assert "longitude" in str(exc.value.detail)


@pytest.mark.unit
class TestParseBbox:
    def test_returns_none_for_none_input(self) -> None:
        assert parse_bbox(None) is None

    def test_parses_well_formed_bbox(self) -> None:
        # GeoJSON ordering: min_lng, min_lat, max_lng, max_lat
        assert parse_bbox("-95.7,29.5,-95.0,30.0") == BoundingBox(
            min_lat=29.5, min_lng=-95.7, max_lat=30.0, max_lng=-95.0
        )

    @pytest.mark.parametrize(
        "raw",
        ["", "1,2,3", "1,2,3,4,5", "a,b,c,d"],
    )
    def test_rejects_malformed_input(self, raw: str) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_bbox(raw)
        assert exc.value.status_code == 400

    def test_rejects_inverted_lat(self) -> None:
        # max_lat (10.0) is less than min_lat (40.0)
        with pytest.raises(HTTPException) as exc:
            parse_bbox("-100,40.0,-90,10.0")
        assert exc.value.status_code == 400
        assert "min_lat" in str(exc.value.detail)

    def test_rejects_antimeridian_crossing(self) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_bbox("170,0,-170,10")
        assert exc.value.status_code == 400
        assert "antimeridian" in str(exc.value.detail)

    def test_rejects_out_of_range_value(self) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_bbox("-200,0,-180,10")
        assert exc.value.status_code == 400


@pytest.mark.unit
class TestParsePolygon:
    def test_returns_none_for_none_input(self) -> None:
        assert parse_polygon(None) is None

    def test_parses_triangle(self) -> None:
        result = parse_polygon("-95.7,29.5,-95.0,29.5,-95.0,30.0")
        assert result == ((-95.7, 29.5), (-95.0, 29.5), (-95.0, 30.0))

    def test_parses_open_quad(self) -> None:
        """Closure is implicit; the parser does not auto-add a vertex."""
        result = parse_polygon("-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0")
        assert result is not None
        assert len(result) == 4

    def test_tolerates_trailing_separator(self) -> None:
        result = parse_polygon("-95.7,29.5,-95.0,29.5,-95.0,30.0,")
        assert result == ((-95.7, 29.5), (-95.0, 29.5), (-95.0, 30.0))

    @pytest.mark.parametrize("raw", ["1,2,3", "1,2,3,4,5"])
    def test_rejects_odd_number_of_components(self, raw: str) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_polygon(raw)
        assert exc.value.status_code == 400
        assert "lng,lat" in str(exc.value.detail)

    def test_rejects_too_few_vertices(self) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_polygon("-95.7,29.5,-95.0,29.5")
        assert exc.value.status_code == 400
        assert "at least 3" in str(exc.value.detail)

    def test_rejects_non_numeric(self) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_polygon("a,b,c,d,e,f")
        assert exc.value.status_code == 400
        assert "numeric" in str(exc.value.detail)

    @pytest.mark.parametrize(
        "raw",
        [
            "200,0,-180,0,-180,10",  # lng > 180
            "0,91,0,-10,10,0",  # lat > 90
            "-181,0,0,0,0,10",  # lng < -180
        ],
    )
    def test_rejects_out_of_range_vertex(self, raw: str) -> None:
        with pytest.raises(HTTPException) as exc:
            parse_polygon(raw)
        assert exc.value.status_code == 400
        assert "out of WGS84 range" in str(exc.value.detail)
