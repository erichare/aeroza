import type { Metadata } from "next";
import Link from "next/link";

import { DocsLayout } from "@/components/DocsLayout";

export const metadata: Metadata = {
  title: "API reference",
  description:
    "Aeroza v1 API reference: every public route, its purpose, and the wire shape.",
};

const API_BASE = process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000";

type HttpMethod = "GET" | "POST" | "PATCH" | "DELETE";

interface Route {
  method: HttpMethod;
  path: string;
  summary: string;
  notes?: string;
}

const ROUTES: ReadonlyArray<{ section: string; routes: ReadonlyArray<Route> }> = [
  {
    section: "Health & meta",
    routes: [
      {
        method: "GET",
        path: "/health",
        summary: "Liveness check.",
        notes: "Returns version + status. No DB or NATS dependency.",
      },
      {
        method: "GET",
        path: "/v1/stats",
        summary: "Compact 'what does the system know right now?' snapshot.",
        notes: "Alert counts, MRMS file/grid counts, freshness watermarks.",
      },
      {
        method: "GET",
        path: "/v1/me",
        summary: "Introspect the calling API key (name, owner, scopes, last-used).",
        notes:
          "Requires Authorization: Bearer aza_live_*. Mint keys with the " +
          "aeroza-api-keys CLI; the operator's only management plane in v1.",
      },
    ],
  },
  {
    section: "Alerts",
    routes: [
      {
        method: "GET",
        path: "/v1/alerts",
        summary: "List active NWS alerts as a GeoJSON FeatureCollection.",
        notes: "Filter by point, bbox, or minimum severity.",
      },
      {
        method: "GET",
        path: "/v1/alerts/stream",
        summary: "Server-Sent Events feed re-emitting newly observed alerts.",
        notes: "One event per row published on aeroza.alerts.nws.new.",
      },
      {
        method: "GET",
        path: "/v1/alerts/{alert_id}",
        summary: "Single-alert detail with description + instruction.",
        notes: "Includes alerts whose `expires` is in the past.",
      },
    ],
  },
  {
    section: "MRMS — catalog & grids",
    routes: [
      {
        method: "GET",
        path: "/v1/mrms/files",
        summary: "MRMS file catalog — what data is available right now.",
        notes: "Filter by product, level, and a [since, until) window.",
      },
      {
        method: "GET",
        path: "/v1/mrms/grids",
        summary: "Materialised-grid catalog — what data is queryable right now.",
        notes: "Same filters as /v1/mrms/files; result is locator-shaped.",
      },
      {
        method: "GET",
        path: "/v1/mrms/grids/{file_key}",
        summary: "One materialised grid by its source S3 key.",
      },
    ],
  },
  {
    section: "MRMS — queries",
    routes: [
      {
        method: "GET",
        path: "/v1/mrms/grids/sample",
        summary: "Nearest-cell value at a (lat, lng).",
        notes:
          "Defaults to the latest grid; pass at_time to reduce a grid valid " +
          "at-or-before that moment.",
      },
      {
        method: "GET",
        path: "/v1/mrms/grids/polygon",
        summary: "Reduce a grid over a polygon (max / mean / min / count_ge).",
        notes:
          "Polygon vertices on lng,lat,lng,lat,…; ring implicitly closed; " +
          "count_ge requires a numeric threshold.",
      },
      {
        method: "GET",
        path: "/v1/mrms/tiles/{z}/{x}/{y}.png",
        summary:
          "Web-Mercator XYZ raster tile of the latest matching MRMS grid.",
        notes:
          "256×256 PNG with the standard NWS dBZ ramp; transparent fallback " +
          "when no grid is materialised. Pass fileKey to pin a specific " +
          "source. Cache-Control: max-age=60.",
      },
    ],
  },
  {
    section: "METAR (surface observations)",
    routes: [
      {
        method: "GET",
        path: "/v1/metar",
        summary: "List recent METAR observations, newest first.",
        notes:
          "Filter by station (case-insensitive ICAO id), since, until, bbox " +
          "(min_lng,min_lat,max_lng,max_lat), and limit (default 100, max 500). " +
          "Sourced from aviationweather.gov; rows include parsed temp/wind/visibility " +
          "plus the raw METAR text for custom parsers.",
      },
      {
        method: "GET",
        path: "/v1/metar/{station_id}/latest",
        summary: "Most-recent observation for one ICAO station.",
        notes: "Case-insensitive on the path. 404 when the station has no observations.",
      },
    ],
  },
  {
    section: "Nowcasts & calibration",
    routes: [
      {
        method: "GET",
        path: "/v1/nowcasts",
        summary: "Predicted-grid catalog (algorithm × forecast horizon).",
        notes:
          "Filter by product, level, algorithm (e.g. 'persistence'), " +
          "horizonMinutes, and a [since, until) window on validAt.",
      },
      {
        method: "GET",
        path: "/v1/calibration",
        summary: "Aggregate verification metrics, grouped by algorithm × horizon.",
        notes:
          "Sample-weighted MAE / bias / RMSE plus categorical POD / FAR / CSI " +
          "(at the verifier's threshold, default 35 dBZ) over the window. Default " +
          "windowHours=24; supports algorithm / product / level filters.",
      },
      {
        method: "GET",
        path: "/v1/calibration/series",
        summary:
          "Time-bucketed companion to /v1/calibration — sparkline per (algorithm, horizon).",
        notes:
          "Same metrics (continuous + categorical), with one extra group-by on " +
          "bucketStart. bucketSeconds in [300, 86400] (default 3600 / 1 hour).",
      },
    ],
  },
  {
    section: "Webhooks",
    routes: [
      {
        method: "GET",
        path: "/v1/webhooks",
        summary: "List webhook subscriptions.",
        notes:
          "Filter by status (active / paused / disabled). The list omits " +
          "the secret; the detail route returns its hash.",
      },
      {
        method: "POST",
        path: "/v1/webhooks",
        summary: "Create a subscription.",
        notes:
          "Body: target URL, events array (e.g. aeroza.alerts.nws.new, " +
          "aeroza.nowcast.grids.new), an optional alertRuleId, optional " +
          "secret (auto-generated if omitted).",
      },
      {
        method: "GET",
        path: "/v1/webhooks/{sub_id}",
        summary: "One subscription's full detail.",
      },
      {
        method: "PATCH",
        path: "/v1/webhooks/{sub_id}",
        summary: "Update events / target / status / alertRuleId.",
        notes:
          "Status transitions: active ⇄ paused; the dispatcher's circuit " +
          "breaker can flip a sub to disabled after repeated 4xx/5xx.",
      },
      {
        method: "DELETE",
        path: "/v1/webhooks/{sub_id}",
        summary: "Soft-delete a subscription.",
        notes:
          "Sets status to deleted; rows are kept so the delivery log " +
          "remains queryable.",
      },
      {
        method: "GET",
        path: "/v1/webhooks/{sub_id}/deliveries",
        summary: "Recent delivery attempts for a subscription.",
        notes:
          "Read-only audit trail — one row per attempt the dispatcher " +
          "made (initial + retries), newest-first. Optional status filter " +
          "(ok / failed / retrying). The signed payload itself is omitted " +
          "from the wire.",
      },
    ],
  },
  {
    section: "Alert rules (webhook predicate DSL)",
    routes: [
      {
        method: "GET",
        path: "/v1/alert-rules",
        summary: "List alert rules.",
        notes:
          "Rules are reusable across webhook subscriptions — one rule, many " +
          "subs. The wire shape is a discriminated union over rule.kind " +
          "(currently 'point' or 'polygon').",
      },
      {
        method: "POST",
        path: "/v1/alert-rules",
        summary: "Create a rule.",
        notes:
          "Body: kind, predicate config (point: lat/lng/radiusMeters; " +
          "polygon: GeoJSON-style coordinates), severity floor, optional " +
          "event-name allowlist.",
      },
      {
        method: "GET",
        path: "/v1/alert-rules/{rule_id}",
        summary: "One rule's full detail.",
      },
      {
        method: "PATCH",
        path: "/v1/alert-rules/{rule_id}",
        summary: "Update predicate / severity floor / status.",
      },
      {
        method: "DELETE",
        path: "/v1/alert-rules/{rule_id}",
        summary: "Soft-delete a rule.",
      },
    ],
  },
];

export default function ApiReferencePage() {
  return (
    <DocsLayout>
      <h1>API reference</h1>
      <p>
        Every public route on the Aeroza v1 API, organised by domain. Use
        this page when you want a one-screen survey of the surface; use the{" "}
        <Link href="/docs/api/explorer">interactive explorer</Link> when you
        want to inspect schemas and send requests directly from the page.
      </p>
      <p>
        Three ways to drive the API interactively:
      </p>
      <ul>
        <li>
          <Link href="/docs/api/explorer">/docs/api/explorer</Link> — the
          embedded Scalar reference (recommended). Themed to match the rest
          of the site, three-column layout, code samples in shell / JS /
          Python out of the box.
        </li>
        <li>
          <a href={`${API_BASE}/docs`} target="_blank" rel="noreferrer">
            {API_BASE}/docs
          </a>{" "}
          — FastAPI's built-in Swagger UI. Plain but always 1:1 with the
          server's current shape.
        </li>
        <li>
          <a href={`${API_BASE}/openapi.json`} target="_blank" rel="noreferrer">
            {API_BASE}/openapi.json
          </a>{" "}
          — the raw OpenAPI schema. Point any codegen tool at it.
        </li>
      </ul>

      <h2>Conventions</h2>
      <ul>
        <li>
          <strong>JSON only on the wire</strong>, with <code>camelCase</code>{" "}
          field aliases on response payloads. The internal Python is
          snake_case; pydantic re-aliases at the boundary.
        </li>
        <li>
          <strong>Geospatial ordering follows GeoJSON / OGC.</strong>{" "}
          <code>bbox</code> and <code>polygon</code> use{" "}
          <code>lng,lat,lng,lat,…</code>; <code>point</code> uses{" "}
          <code>lat,lng</code> for human-friendliness.
        </li>
        <li>
          <strong>Times are ISO-8601 UTC.</strong> When an endpoint returns a{" "}
          <code>validAt</code> or <code>materialisedAt</code>, expect a{" "}
          <code>Z</code> suffix.
        </li>
        <li>
          <strong>Errors are FastAPI-shaped:</strong> JSON body with a{" "}
          <code>detail</code> field. Out-of-domain queries return 404 with a
          human-readable detail; validation failures return 422.
        </li>
      </ul>

      {ROUTES.map((section) => (
        <RouteSection key={section.section} {...section} />
      ))}

      <h2>Stable contract?</h2>
      <p>
        v1 is the contract <em>shape</em> we're committing to — JSON envelope,
        camelCase aliases, GeoJSON ordering. Adding new fields to a response
        payload is non-breaking; removing or renaming will bump the route to{" "}
        <code>/v2</code>. The TypeScript SDK (<code>@aeroza/sdk</code>) pins
        the wire types so consumers get a compile-time signal when something
        changes; the dev console at <Link href="/console">/console</Link> is
        the same SDK driving every panel, so any awkwardness in the contract
        shows up in the SDK first.
      </p>
    </DocsLayout>
  );
}

function RouteSection({ section, routes }: { section: string; routes: ReadonlyArray<Route> }) {
  return (
    <>
      <h2>{section}</h2>
      <table>
        <thead>
          <tr>
            <th>Route</th>
            <th>Summary</th>
          </tr>
        </thead>
        <tbody>
          {routes.map((r) => (
            <tr key={r.path}>
              <td>
                <code>{r.method} {r.path}</code>
              </td>
              <td>
                <div>{r.summary}</div>
                {r.notes ? (
                  <div className="mt-1 text-[11px] text-muted">{r.notes}</div>
                ) : null}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
