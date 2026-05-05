"""Unit tests for the tile route's Accept-header negotiation.

The helper is private to ``aeroza.query.v1.mrms`` but is the single
choke-point for "PNG vs WebP" — worth pinning the contract directly
rather than only through the integration tests, which require a real
DB and Zarr fixture to exercise.
"""

from __future__ import annotations

import pytest

from aeroza.query.v1.mrms import _negotiate_tile_format

pytestmark = pytest.mark.unit


def test_no_accept_header_defaults_to_webp() -> None:
    """Browser ``fetch()`` and MapLibre's raster source send no
    custom Accept (or just ``*/*``) — both should land on WebP so
    the deployed radar UI gets the smaller bytes."""
    assert _negotiate_tile_format(None) == "webp"


def test_star_star_accepts_webp() -> None:
    assert _negotiate_tile_format("*/*") == "webp"


def test_image_star_accepts_webp() -> None:
    assert _negotiate_tile_format("image/*") == "webp"


def test_explicit_webp_returns_webp() -> None:
    assert _negotiate_tile_format("image/webp,image/png,*/*") == "webp"


def test_png_only_returns_png() -> None:
    """Curl smoke tests + older non-browser clients that explicitly
    ask for PNG and reject WebP get PNG. This is the only path that
    keeps PNG live after the default flip."""
    assert _negotiate_tile_format("image/png") == "png"


def test_png_first_but_with_webp_returns_webp() -> None:
    """When both are listed we prefer WebP regardless of the order in
    the Accept header — modern browsers send PNG first historically
    but always tolerate WebP."""
    assert _negotiate_tile_format("image/png,image/webp") == "webp"


def test_case_insensitive() -> None:
    assert _negotiate_tile_format("IMAGE/WEBP") == "webp"
    assert _negotiate_tile_format("Image/PNG") == "png"


def test_unknown_accept_value_falls_back_to_webp() -> None:
    """An exotic Accept that mentions neither PNG nor WebP shouldn't
    block the request — pick WebP (the default) and let the client
    decide whether to render it."""
    assert _negotiate_tile_format("application/octet-stream") == "webp"
