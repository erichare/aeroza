"""HMAC signing primitive for outbound webhook deliveries.

Stripe-style: the dispatcher (PR #6) attaches two headers to every
delivery, ``Aeroza-Timestamp`` and ``Aeroza-Signature``. The signature
is ``HMAC-SHA256(secret, timestamp + "." + payload)`` hex-encoded.
Consumers verify by recomputing on receipt and comparing in constant
time, with a configurable freshness window to defeat replays.

The format includes the timestamp in the signed payload (rather than
trusting the header alone) so reordering or rewriting the timestamp
header invalidates the signature. The format prefix is versioned
(``"v1=" + hex``) so a future MAC algorithm can land without ambiguity.

This module is deliberately I/O-free: no HTTP, no DB. It's exercised
by unit tests in this PR; the dispatcher worker wires it up in PR #6.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from dataclasses import dataclass
from typing import Final

# Wire constants. Keep these as module-level finals so tests can import
# them without coupling to the wire format string.
SIGNATURE_HEADER: Final[str] = "Aeroza-Signature"
TIMESTAMP_HEADER: Final[str] = "Aeroza-Timestamp"
SIGNATURE_VERSION: Final[str] = "v1"
SECRET_BYTES: Final[int] = 32  # 256 bits
DEFAULT_FRESHNESS_S: Final[int] = 5 * 60  # 5 minutes — replay window


class SignatureError(ValueError):
    """Raised by :func:`verify_signature` when verification fails.

    A single exception type covers every failure mode (malformed
    header, version mismatch, MAC mismatch, stale timestamp); callers
    treat them uniformly as "drop the request" rather than branching
    on cause. The string carries enough detail for logging.
    """


@dataclass(frozen=True, slots=True)
class SignedHeaders:
    """The headers a dispatcher attaches to a delivery.

    Returned by :func:`sign_payload`. Tuple-shaped so tests can read
    the timestamp + signature without indexing into a dict.
    """

    timestamp: str
    signature: str

    def as_dict(self) -> dict[str, str]:
        """The headers as an HTTP-ready ``{name: value}`` dict."""
        return {
            TIMESTAMP_HEADER: self.timestamp,
            SIGNATURE_HEADER: self.signature,
        }


def generate_secret() -> str:
    """Return a fresh hex-encoded HMAC secret.

    32 bytes / 64 hex chars; well above the 256-bit security baseline
    SHA-256 needs. Generated via :mod:`secrets` so it's CSPRNG-backed
    even in adversarial environments.
    """
    return secrets.token_hex(SECRET_BYTES)


def sign_payload(
    *,
    payload: bytes,
    secret: str,
    timestamp: int | None = None,
) -> SignedHeaders:
    """Sign ``payload`` with ``secret``. Returns ready-to-send headers.

    ``timestamp`` is the seconds-since-epoch the signature is anchored
    to. Defaults to ``int(time.time())``; tests pass an explicit value
    to make assertions deterministic.

    The signed message is ``f"{timestamp}.{payload-as-utf8}"`` — same
    shape as Stripe's so consumers familiar with that contract can
    map across.
    """
    ts = int(time.time()) if timestamp is None else int(timestamp)
    signed_string = f"{ts}.".encode() + payload
    digest = hmac.new(
        secret.encode("utf-8"),
        signed_string,
        hashlib.sha256,
    ).hexdigest()
    return SignedHeaders(
        timestamp=str(ts),
        signature=f"{SIGNATURE_VERSION}={digest}",
    )


def verify_signature(
    *,
    payload: bytes,
    secret: str,
    timestamp_header: str | None,
    signature_header: str | None,
    now: int | None = None,
    freshness_s: int = DEFAULT_FRESHNESS_S,
) -> None:
    """Raise :class:`SignatureError` if the headers don't authenticate ``payload``.

    Returns ``None`` on success — callers wrap in try/except. Same
    rationale as the Python convention for ``hmac.compare_digest``: a
    boolean return tempts callers to forget to handle ``False``.

    Verifies four invariants in order:

    1. Both headers are present.
    2. ``signature_header`` parses as ``"<version>=<hex>"``, version
       matches :data:`SIGNATURE_VERSION`.
    3. ``timestamp_header`` parses as a positive integer and is within
       ``freshness_s`` seconds of ``now`` (defeats replays of an old
       signed payload). ``now`` defaults to ``int(time.time())``;
       tests inject a fixed value.
    4. The MAC matches in constant time (:func:`hmac.compare_digest`).
    """
    if not timestamp_header or not signature_header:
        raise SignatureError("missing signature headers")

    try:
        ts = int(timestamp_header)
    except ValueError as exc:
        raise SignatureError("timestamp header is not an integer") from exc
    if ts <= 0:
        raise SignatureError("timestamp header must be positive")

    current = int(time.time()) if now is None else int(now)
    if abs(current - ts) > freshness_s:
        raise SignatureError(f"timestamp {ts} is outside the {freshness_s}s freshness window")

    version, _, provided_hex = signature_header.partition("=")
    if not provided_hex or version != SIGNATURE_VERSION:
        raise SignatureError(f"signature must start with '{SIGNATURE_VERSION}='")

    expected = hmac.new(
        secret.encode("utf-8"),
        f"{ts}.".encode() + payload,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, provided_hex):
        raise SignatureError("signature mismatch")


__all__ = [
    "DEFAULT_FRESHNESS_S",
    "SECRET_BYTES",
    "SIGNATURE_HEADER",
    "SIGNATURE_VERSION",
    "TIMESTAMP_HEADER",
    "SignatureError",
    "SignedHeaders",
    "generate_secret",
    "sign_payload",
    "verify_signature",
]
