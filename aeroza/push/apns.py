"""Token-based APNs sender (HTTP/2 + ES256 provider JWT).

Hand-rolled rather than pulling a push library: APNs token auth is a small,
stable protocol, and owning it keeps the dependency surface to ``httpx[http2]``
+ ``cryptography`` (both already needed elsewhere) and makes the whole send
path unit-testable with ``respx``.

The provider token (a short JWT signed by the ``.p8`` key) is cached and reused
— APNs rejects tokens regenerated more than once every ~20 min
(``TooManyProviderTokenUpdates``) and requires refresh at least hourly, so we
refresh on a fixed interval comfortably inside that band.

Reference: Apple, "Establishing a token-based connection to APNs".
"""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final, Protocol

import httpx
import structlog
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

log = structlog.get_logger(__name__)

APNS_PRODUCTION_HOST: Final[str] = "api.push.apple.com"
APNS_SANDBOX_HOST: Final[str] = "api.sandbox.push.apple.com"

# Refresh the provider token every 40 min: inside APNs' 60-min max age, and well
# above the ~20-min minimum between regenerations.
_PROVIDER_TOKEN_TTL_SECONDS: Final[float] = 40 * 60

# Reasons APNs returns when a token is permanently dead — prune the device.
_UNREGISTERED_REASONS: Final[frozenset[str]] = frozenset({"Unregistered", "BadDeviceToken"})


@dataclass(frozen=True, slots=True)
class ApnsSettings:
    """Everything needed to sign + address an APNs request."""

    key_id: str
    team_id: str
    private_key_pem: str
    topic: str
    use_sandbox: bool = False


@dataclass(frozen=True, slots=True)
class ApnsResult:
    """Outcome of one APNs send."""

    status_code: int
    apns_id: str | None = None
    reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.status_code == 200

    @property
    def unregistered(self) -> bool:
        """True when the token is permanently invalid and should be pruned."""
        return self.status_code == 410 or (self.reason in _UNREGISTERED_REASONS)


class PushSender(Protocol):
    """Narrow surface the dispatch layer needs — lets tests inject a fake."""

    async def send(
        self, *, device_token: str, environment: str, payload: dict[str, Any]
    ) -> ApnsResult: ...


def load_apns_private_key(raw: str) -> ec.EllipticCurvePrivateKey:
    """Load the ``.p8`` EC key from raw PEM or a base64-of-the-file blob.

    Accepting both means an operator can paste the multi-line PEM directly, or
    a single-line ``base64 -i AuthKey.p8`` when an env UI mangles newlines.
    """
    text = raw.strip()
    if "BEGIN" not in text:
        text = base64.b64decode(text).decode("utf-8")
    key = serialization.load_pem_private_key(text.encode("utf-8"), password=None)
    if not isinstance(key, ec.EllipticCurvePrivateKey):
        raise ValueError("APNs private key must be an EC (P-256) key")
    return key


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _es256_signature(key: ec.EllipticCurvePrivateKey, signing_input: bytes) -> bytes:
    """Sign with ES256 and return the JWS-format raw ``r || s`` (64 bytes)."""
    der = key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der)
    return r.to_bytes(32, "big") + s.to_bytes(32, "big")


def build_provider_jwt(
    key: ec.EllipticCurvePrivateKey, *, key_id: str, team_id: str, issued_at: int
) -> str:
    """Build a signed APNs provider JWT (header.payload.signature)."""
    header = _b64url(json.dumps({"alg": "ES256", "kid": key_id}, separators=(",", ":")).encode())
    claims = _b64url(json.dumps({"iss": team_id, "iat": issued_at}, separators=(",", ":")).encode())
    signing_input = f"{header}.{claims}".encode("ascii")
    signature = _b64url(_es256_signature(key, signing_input))
    return f"{header}.{claims}.{signature}"


class _ProviderTokenCache:
    """Caches the signed provider JWT, regenerating only after the TTL."""

    def __init__(
        self,
        key: ec.EllipticCurvePrivateKey,
        *,
        key_id: str,
        team_id: str,
        ttl_seconds: float = _PROVIDER_TOKEN_TTL_SECONDS,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._key = key
        self._key_id = key_id
        self._team_id = team_id
        self._ttl = ttl_seconds
        self._now = now
        self._token: str | None = None
        self._issued_at: float = 0.0

    def token(self) -> str:
        now = self._now()
        if self._token is None or (now - self._issued_at) >= self._ttl:
            self._token = build_provider_jwt(
                self._key, key_id=self._key_id, team_id=self._team_id, issued_at=int(now)
            )
            self._issued_at = now
        return self._token


class ApnsClient:
    """Sends alert pushes to APNs over HTTP/2 with a cached provider token."""

    def __init__(
        self,
        settings: ApnsSettings,
        *,
        client: httpx.AsyncClient | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._settings = settings
        self._tokens = _ProviderTokenCache(
            load_apns_private_key(settings.private_key_pem),
            key_id=settings.key_id,
            team_id=settings.team_id,
            now=now,
        )
        self._client = client
        self._owns_client = client is None

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            # APNs requires HTTP/2; `h2` ships via the httpx[http2] extra.
            self._client = httpx.AsyncClient(http2=True, timeout=httpx.Timeout(10.0))
        return self._client

    def _host(self, environment: str) -> str:
        if self._settings.use_sandbox or environment == "sandbox":
            return APNS_SANDBOX_HOST
        return APNS_PRODUCTION_HOST

    async def send(
        self, *, device_token: str, environment: str, payload: dict[str, Any]
    ) -> ApnsResult:
        client = await self._http()
        url = f"https://{self._host(environment)}/3/device/{device_token}"
        headers = {
            "authorization": f"bearer {self._tokens.token()}",
            "apns-topic": self._settings.topic,
            "apns-push-type": "alert",
            "apns-priority": "10",
        }
        try:
            response = await client.post(url, headers=headers, json=payload)
        except httpx.HTTPError as exc:
            log.warning("apns.send.transport_error", error=str(exc))
            return ApnsResult(status_code=0, reason=str(exc))

        reason: str | None = None
        if response.status_code != 200:
            try:
                reason = response.json().get("reason")
            except (json.JSONDecodeError, ValueError):
                reason = None
        return ApnsResult(
            status_code=response.status_code,
            apns_id=response.headers.get("apns-id"),
            reason=reason,
        )

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None
