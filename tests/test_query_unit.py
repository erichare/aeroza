"""Unit tests for query helpers — no DB required."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from aeroza.ingest.nws_alerts import Severity, severities_at_least, severity_rank
from aeroza.query.parsers import parse_point
from aeroza.shared.types import Coordinate


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
