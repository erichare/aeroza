"""Unit tests for the APNs sender — no live key, no network (respx-mocked)."""

from __future__ import annotations

import base64
import json

import httpx
import pytest
import respx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

from aeroza.push.apns import (
    APNS_PRODUCTION_HOST,
    APNS_SANDBOX_HOST,
    ApnsClient,
    ApnsSettings,
    _ProviderTokenCache,
    build_provider_jwt,
    load_apns_private_key,
)


def _gen_key() -> ec.EllipticCurvePrivateKey:
    return ec.generate_private_key(ec.SECP256R1())


def _pem(key: ec.EllipticCurvePrivateKey) -> str:
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")


def _settings(key: ec.EllipticCurvePrivateKey, *, use_sandbox: bool = False) -> ApnsSettings:
    return ApnsSettings(
        key_id="ABC1234567",
        team_id="TEAM123456",
        private_key_pem=_pem(key),
        topic="app.aeroza.AerozaOne",
        use_sandbox=use_sandbox,
    )


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def test_load_key_accepts_pem_and_base64() -> None:
    pem = _pem(_gen_key())
    assert isinstance(load_apns_private_key(pem), ec.EllipticCurvePrivateKey)
    b64 = base64.b64encode(pem.encode()).decode()
    assert isinstance(load_apns_private_key(b64), ec.EllipticCurvePrivateKey)


def test_load_key_rejects_non_ec() -> None:
    rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = rsa_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    with pytest.raises(ValueError):
        load_apns_private_key(pem)


def test_provider_jwt_is_verifiable_es256() -> None:
    key = _gen_key()
    token = build_provider_jwt(
        key, key_id="KID0000001", team_id="TEAM000001", issued_at=1_700_000_000
    )
    header_b64, claims_b64, sig_b64 = token.split(".")
    assert json.loads(_b64url_decode(header_b64)) == {"alg": "ES256", "kid": "KID0000001"}
    assert json.loads(_b64url_decode(claims_b64)) == {"iss": "TEAM000001", "iat": 1_700_000_000}

    raw_sig = _b64url_decode(sig_b64)
    assert len(raw_sig) == 64
    der = encode_dss_signature(
        int.from_bytes(raw_sig[:32], "big"), int.from_bytes(raw_sig[32:], "big")
    )
    # Raises cryptography.exceptions.InvalidSignature if the signature is wrong.
    key.public_key().verify(
        der, f"{header_b64}.{claims_b64}".encode("ascii"), ec.ECDSA(hashes.SHA256())
    )


def test_provider_token_cache_reuses_then_refreshes() -> None:
    clock = {"t": 1000.0}
    cache = _ProviderTokenCache(
        _gen_key(), key_id="KID", team_id="TEAM", ttl_seconds=100.0, now=lambda: clock["t"]
    )
    first = cache.token()
    clock["t"] += 50  # still within TTL
    assert cache.token() == first
    clock["t"] += 60  # 110s elapsed > 100s TTL
    assert cache.token() != first


@respx.mock
async def test_send_success_production_host() -> None:
    token = "a" * 64
    route = respx.post(f"https://{APNS_PRODUCTION_HOST}/3/device/{token}").mock(
        return_value=httpx.Response(200, headers={"apns-id": "XYZ"})
    )
    client = ApnsClient(_settings(_gen_key()))
    result = await client.send(
        device_token=token, environment="production", payload={"aps": {"alert": "x"}}
    )
    await client.aclose()
    assert route.called
    assert result.ok
    assert result.apns_id == "XYZ"
    assert not result.unregistered


@respx.mock
async def test_send_uses_sandbox_host_for_sandbox_device() -> None:
    token = "b" * 64
    route = respx.post(f"https://{APNS_SANDBOX_HOST}/3/device/{token}").mock(
        return_value=httpx.Response(200)
    )
    client = ApnsClient(_settings(_gen_key()))
    await client.send(device_token=token, environment="sandbox", payload={"aps": {}})
    await client.aclose()
    assert route.called


@respx.mock
async def test_send_410_marks_unregistered() -> None:
    token = "c" * 64
    respx.post(f"https://{APNS_PRODUCTION_HOST}/3/device/{token}").mock(
        return_value=httpx.Response(410, json={"reason": "Unregistered"})
    )
    client = ApnsClient(_settings(_gen_key()))
    result = await client.send(device_token=token, environment="production", payload={"aps": {}})
    await client.aclose()
    assert result.status_code == 410
    assert result.unregistered
