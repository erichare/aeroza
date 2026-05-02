"""Webhook delivery: subscriptions + dispatcher.

This package owns the operator-facing surface (CRUD over
``webhook_subscriptions``) and the wire-side primitives the dispatcher
worker uses to actually fan events out to external endpoints.

Phase 4 lands across three PRs:

1. **Subscription model** (this PR) — DB schema, ORM, pydantic shapes,
   HMAC signing primitive, and ``/v1/webhooks/*`` CRUD routes.
2. **Alert-rule DSL** — a small Datadog-monitor-style grammar for
   "when grid value at point P matches predicate, fire". Persists
   alongside subscriptions; bound to a subscription via foreign key.
3. **Dispatcher worker** — long-running consumer that reads NATS
   events, fans out to matching subscriptions via signed POST, retries
   on failure, and disables a subscription after sustained failures.

Event taxonomy on the wire mirrors the NATS subjects exactly:

- ``aeroza.alerts.nws.new``
- ``aeroza.mrms.files.new``
- ``aeroza.mrms.grids.new``

That keeps logs, NATS, the SDK, and the webhook payload all speaking
the same vocabulary. Stripping the ``aeroza.`` prefix on the wire was
considered and rejected — when the dispatcher tags every delivery
with the originating subject, having the same string in the broker
log makes incident triage one grep away.
"""

from aeroza.webhooks.signing import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    SignatureError,
    sign_payload,
    verify_signature,
)

__all__ = [
    "SIGNATURE_HEADER",
    "TIMESTAMP_HEADER",
    "SignatureError",
    "sign_payload",
    "verify_signature",
]
