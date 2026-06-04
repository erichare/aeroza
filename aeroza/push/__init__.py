"""Push-notification subsystem: device registration + APNs dispatch.

Devices register their APNs token and saved location via ``/v1/push/devices``
(:mod:`aeroza.push.routes`). When a new NWS warning is ingested, the
:class:`aeroza.push.dispatch.PushDispatchPublisher` decorator finds the
registered devices whose saved point falls inside the warning polygon (PostGIS
``ST_Intersects``) and sends each a lean APNs alert via :mod:`aeroza.push.apns`.
The iOS Notification Service Extension then hydrates the push with a fresh
reflectivity sample at the saved coordinate.

Registration is anonymous by default — matching the app's no-accounts stance.
"""

from __future__ import annotations
