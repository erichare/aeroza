import type { Metadata } from "next";

import { DocsLayout } from "@/components/DocsLayout";

export const metadata: Metadata = {
  title: "API reference",
  description:
    "Aeroza v1 API reference: every public route, its purpose, and the wire shape.",
};

const API_BASE = process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000";

interface Route {
  method: "GET";
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
          "Sample-weighted MAE / bias / RMSE over the window. Default " +
          "windowHours=24; supports algorithm / product / level filters.",
      },
    ],
  },
];

export default function ApiReferencePage() {
  return (
    <DocsLayout>
      <h1>API reference</h1>
      <p>
        Every public route on the Aeroza v1 API. The interactive request
        builder lives at{" "}
        <a href={`${API_BASE}/docs`} target="_blank" rel="noreferrer">
          {API_BASE}/docs
        </a>{" "}
        (Swagger UI, auto-generated from FastAPI). The raw schema is at{" "}
        <a href={`${API_BASE}/openapi.json`} target="_blank" rel="noreferrer">
          {API_BASE}/openapi.json
        </a>{" "}
        — point any codegen tool at it.
      </p>

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
        <code>/v2</code>. The TypeScript SDK (<code>@aeroza/sdk</code>) lands
        next and pins the wire types so consumers get a compile-time signal
        when something changes.
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
