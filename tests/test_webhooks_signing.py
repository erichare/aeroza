"""Unit tests for the HMAC signing primitive.

The signing module is the load-bearing piece of webhook delivery —
the dispatcher (PR #6) builds on it and consumers verify on receipt.
These tests pin the wire format (header names, version prefix, MAC
shape), the freshness window, and the failure modes that map to
``SignatureError``.
"""

from __future__ import annotations

import pytest

from aeroza.webhooks.signing import (
    DEFAULT_FRESHNESS_S,
    SIGNATURE_HEADER,
    SIGNATURE_VERSION,
    TIMESTAMP_HEADER,
    SignatureError,
    generate_secret,
    sign_payload,
    verify_signature,
)

pytestmark = pytest.mark.unit


def test_signature_header_constants_match_wire_format() -> None:
    assert SIGNATURE_HEADER == "Aeroza-Signature"
    assert TIMESTAMP_HEADER == "Aeroza-Timestamp"
    assert SIGNATURE_VERSION == "v1"


def test_generate_secret_returns_64_hex_chars() -> None:
    """32 bytes hex-encoded = 64 chars."""
    secret = generate_secret()
    assert len(secret) == 64
    int(secret, 16)  # raises ValueError if not hex


def test_generate_secret_is_unique_each_call() -> None:
    assert generate_secret() != generate_secret()


def test_sign_payload_uses_provided_timestamp() -> None:
    headers = sign_payload(payload=b'{"hello":"world"}', secret="s", timestamp=1700)
    assert headers.timestamp == "1700"
    assert headers.signature.startswith("v1=")


def test_sign_payload_signature_is_deterministic_for_same_inputs() -> None:
    a = sign_payload(payload=b"x", secret="k", timestamp=42)
    b = sign_payload(payload=b"x", secret="k", timestamp=42)
    assert a.signature == b.signature


def test_sign_payload_signature_changes_when_payload_changes() -> None:
    a = sign_payload(payload=b"a", secret="k", timestamp=42)
    b = sign_payload(payload=b"b", secret="k", timestamp=42)
    assert a.signature != b.signature


def test_sign_payload_signature_changes_when_secret_changes() -> None:
    a = sign_payload(payload=b"x", secret="k1", timestamp=42)
    b = sign_payload(payload=b"x", secret="k2", timestamp=42)
    assert a.signature != b.signature


def test_sign_payload_signature_changes_when_timestamp_changes() -> None:
    """Including the timestamp in the signed string defeats header-rewrite attacks."""
    a = sign_payload(payload=b"x", secret="k", timestamp=42)
    b = sign_payload(payload=b"x", secret="k", timestamp=43)
    assert a.signature != b.signature


def test_signed_headers_as_dict_returns_both_headers() -> None:
    headers = sign_payload(payload=b"p", secret="k", timestamp=10)
    rendered = headers.as_dict()
    assert rendered == {
        TIMESTAMP_HEADER: "10",
        SIGNATURE_HEADER: headers.signature,
    }


def test_verify_accepts_well_formed_recent_signature() -> None:
    payload = b'{"event":"aeroza.alerts.nws.new"}'
    headers = sign_payload(payload=payload, secret="k", timestamp=1_000_000)
    verify_signature(
        payload=payload,
        secret="k",
        timestamp_header=headers.timestamp,
        signature_header=headers.signature,
        now=1_000_000,
    )


def test_verify_rejects_missing_headers() -> None:
    with pytest.raises(SignatureError, match="missing"):
        verify_signature(payload=b"p", secret="k", timestamp_header=None, signature_header="v1=abc")
    with pytest.raises(SignatureError, match="missing"):
        verify_signature(payload=b"p", secret="k", timestamp_header="42", signature_header=None)


def test_verify_rejects_non_integer_timestamp() -> None:
    with pytest.raises(SignatureError, match="integer"):
        verify_signature(
            payload=b"p",
            secret="k",
            timestamp_header="not-a-number",
            signature_header="v1=abc",
        )


def test_verify_rejects_negative_timestamp() -> None:
    with pytest.raises(SignatureError, match="positive"):
        verify_signature(
            payload=b"p",
            secret="k",
            timestamp_header="-5",
            signature_header="v1=abc",
        )


def test_verify_rejects_stale_timestamp() -> None:
    payload = b"p"
    headers = sign_payload(payload=payload, secret="k", timestamp=100)
    with pytest.raises(SignatureError, match="freshness window"):
        verify_signature(
            payload=payload,
            secret="k",
            timestamp_header=headers.timestamp,
            signature_header=headers.signature,
            now=100 + DEFAULT_FRESHNESS_S + 1,
            freshness_s=DEFAULT_FRESHNESS_S,
        )


def test_verify_rejects_future_timestamp_outside_window() -> None:
    """Symmetric clock skew check — future-tagged signatures also rejected."""
    payload = b"p"
    headers = sign_payload(payload=payload, secret="k", timestamp=100 + 1000)
    with pytest.raises(SignatureError, match="freshness window"):
        verify_signature(
            payload=payload,
            secret="k",
            timestamp_header=headers.timestamp,
            signature_header=headers.signature,
            now=100,
            freshness_s=300,
        )


def test_verify_rejects_unknown_signature_version() -> None:
    headers = sign_payload(payload=b"p", secret="k", timestamp=1_000_000)
    bad = headers.signature.replace("v1=", "v2=", 1)
    with pytest.raises(SignatureError, match="signature must start with"):
        verify_signature(
            payload=b"p",
            secret="k",
            timestamp_header=headers.timestamp,
            signature_header=bad,
            now=1_000_000,
        )


def test_verify_rejects_wrong_secret() -> None:
    payload = b"p"
    headers = sign_payload(payload=payload, secret="right", timestamp=1_000_000)
    with pytest.raises(SignatureError, match="mismatch"):
        verify_signature(
            payload=payload,
            secret="wrong",
            timestamp_header=headers.timestamp,
            signature_header=headers.signature,
            now=1_000_000,
        )


def test_verify_rejects_tampered_payload() -> None:
    headers = sign_payload(payload=b"original", secret="k", timestamp=1_000_000)
    with pytest.raises(SignatureError, match="mismatch"):
        verify_signature(
            payload=b"tampered",
            secret="k",
            timestamp_header=headers.timestamp,
            signature_header=headers.signature,
            now=1_000_000,
        )
