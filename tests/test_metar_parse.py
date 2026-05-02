"""Unit tests for the AWC METAR JSON parser.

Pure function tests — no DB, no HTTP. The AWC API returns
already-parsed records; the parser's job is to coerce mixed-type
fields (visibility, wind direction) and drop malformed rows without
crashing.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from aeroza.ingest.metar import parse_awc_response

pytestmark = pytest.mark.unit


def _record(**overrides: object) -> dict[str, object]:
    """Minimal valid AWC record. Override fields per test."""
    base: dict[str, object] = {
        "icaoId": "KIAH",
        "reportTime": "2026-05-02T18:00:00Z",
        "lat": 29.98,
        "lon": -95.34,
        "rawOb": "KIAH 021800Z 18012KT 10SM CLR 32/22 A2998",
        "temp": 32.0,
        "dewp": 22.0,
        "wspd": 12.0,
        "wdir": 180,
        "visib": 10.0,
        "altim": 1015.0,
        "fltcat": "VFR",
    }
    base.update(overrides)
    return base


def test_minimal_payload_round_trips() -> None:
    out = parse_awc_response([_record()])
    assert len(out) == 1
    obs = out[0]
    assert obs.station_id == "KIAH"
    assert obs.observation_time == datetime(2026, 5, 2, 18, 0, tzinfo=UTC)
    assert obs.wind_speed_kt == 12.0
    assert obs.flight_category == "VFR"


def test_empty_payload_returns_empty_tuple() -> None:
    assert parse_awc_response([]) == ()


def test_missing_optional_fields_become_none() -> None:
    record = _record()
    for key in ("temp", "dewp", "wspd", "wdir", "wgst", "visib", "altim", "fltcat"):
        record.pop(key, None)
    out = parse_awc_response([record])
    assert len(out) == 1
    obs = out[0]
    assert obs.temp_c is None
    assert obs.wind_direction_deg is None
    assert obs.visibility_sm is None
    assert obs.flight_category is None


def test_variable_wind_direction_flattens_to_none() -> None:
    """AWC returns ``"VRB"`` for variable winds — there's no scalar
    representation so we flatten to None rather than fabricate one."""
    out = parse_awc_response([_record(wdir="VRB")])
    assert out[0].wind_direction_deg is None


def test_visibility_ten_plus_normalises_to_ten() -> None:
    """``"10+"`` (≥10 SM) is the most common visibility code; floor
    to 10.0 so callers can treat the field as a plain float."""
    out = parse_awc_response([_record(visib="10+")])
    assert out[0].visibility_sm == 10.0


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1/4", 0.25),
        ("1/2", 0.5),
        ("3/4", 0.75),
        ("M1/4", 0.0),
        (5.0, 5.0),
        ("garbage", None),
    ],
)
def test_visibility_string_coercions(raw: object, expected: float | None) -> None:
    out = parse_awc_response([_record(visib=raw)])
    assert out[0].visibility_sm == expected


def test_record_missing_load_bearing_field_is_dropped() -> None:
    """A record with no ``rawOb`` is unusable — the parser drops it."""
    bad = _record(rawOb="")
    good = _record(icaoId="KHOU")
    out = parse_awc_response([bad, good])
    assert len(out) == 1
    assert out[0].station_id == "KHOU"


def test_record_with_bad_timestamp_is_dropped() -> None:
    out = parse_awc_response([_record(reportTime="not-a-timestamp")])
    assert out == ()


def test_record_with_extra_unknown_fields_still_parses() -> None:
    """AWC may add fields; the parser must not break when it sees
    something it doesn't know about."""
    record = _record(some_future_field="ignore-me", another=42)
    out = parse_awc_response([record])
    assert len(out) == 1
