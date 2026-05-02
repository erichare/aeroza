import type { Metadata } from "next";
import Link from "next/link";

import { DocsLayout } from "@/components/DocsLayout";

export const metadata: Metadata = {
  title: "Concepts",
  description:
    "The data model behind Aeroza: NWS alerts, MRMS files, materialised grids, " +
    "point sample, polygon reducers, and how they fit together.",
};

export default function ConceptsPage() {
  return (
    <DocsLayout>
      <h1>Concepts</h1>
      <p>
        Aeroza's surface is small on purpose. There are two domains —{" "}
        <strong>alerts</strong> and <strong>radar grids</strong> — and four
        ways to query them: list, single-detail, point sample, and polygon
        reduction. This page explains each piece end-to-end.
      </p>

      <h2>NWS alerts</h2>
      <p>
        Alerts come from the National Weather Service public API and are
        normalised into a flat schema with a five-level <code>severity</code>{" "}
        ladder: <code>Unknown</code> → <code>Minor</code> →{" "}
        <code>Moderate</code> → <code>Severe</code> → <code>Extreme</code>.
        Every alert carries a polygon (or a fallback bbox) so geospatial
        filters work uniformly.
      </p>
      <ul>
        <li>
          <strong>List:</strong> <code>GET /v1/alerts</code> returns active
          alerts as a GeoJSON <code>FeatureCollection</code>, filterable by{" "}
          <code>point</code>, <code>bbox</code>, or minimum <code>severity</code>.
        </li>
        <li>
          <strong>Stream:</strong> <code>GET /v1/alerts/stream</code> is a
          Server-Sent Events feed re-emitting newly observed alerts published
          on the <code>aeroza.alerts.nws.new</code> NATS subject. Use this for
          real-time dashboards.
        </li>
        <li>
          <strong>Detail:</strong> <code>GET /v1/alerts/{`{id}`}</code> returns
          one alert with the long-form <code>description</code> and{" "}
          <code>instruction</code> fields the list endpoint omits.
        </li>
      </ul>

      <h2>MRMS files (the catalog)</h2>
      <p>
        MRMS — Multi-Radar / Multi-Sensor — is NOAA's blended CONUS radar
        product, published as gzipped GRIB2 files on AWS Open Data every
        ~2 minutes. The <code>aeroza-ingest-mrms</code> worker lists the
        bucket and persists a row per file: <code>key</code>,{" "}
        <code>product</code>, <code>level</code>, <code>validAt</code>,{" "}
        <code>sizeBytes</code>, and <code>etag</code>. The catalog is the
        "what data is available right now" feed.
      </p>
      <p>
        <strong>Why catalog before payload?</strong> The discovery step is
        cheap (one S3 list call) and never fails the way decoding can.
        Decoupling it from materialisation means a missing system library or
        a malformed GRIB doesn't silence the freshness signal.
      </p>

      <h2>Materialised grids (the queryable layer)</h2>
      <p>
        The <code>aeroza-materialise-mrms</code> worker decodes each
        catalogued GRIB2 with <code>cfgrib</code> + <code>eccodes</code>,
        writes it to a Zarr store, and records the locator (URI, variable,
        shape, dtype, nbytes) in the <code>mrms_grids</code> table. It
        triggers two ways:
      </p>
      <ol>
        <li>
          <strong>Event:</strong> subscribes to{" "}
          <code>aeroza.mrms.files.new</code> and runs a tick per arriving
          file event — fresh data lands as a queryable grid in seconds.
        </li>
        <li>
          <strong>Backstop interval:</strong> a 60s scheduler also runs the
          same catalog-scan tick, so missed events / cold starts catch up
          on the next sweep.
        </li>
      </ol>
      <p>
        Successful materialisations publish{" "}
        <code>aeroza.mrms.grids.new</code>, which downstream consumers
        (nowcasting, alerts, webhooks) can subscribe to.
      </p>

      <h2>Point sample</h2>
      <p>
        <code>GET /v1/mrms/grids/sample?lat=&amp;lng=</code> returns the
        nearest-cell value for a point against the latest grid (or one valid
        at-or-before <code>at_time</code>). Three things to know:
      </p>
      <ul>
        <li>
          <strong>Tolerance.</strong> By default the request 404s if no cell
          centre is within <code>0.05°</code> of the point — bare nearest-
          neighbour would happily return a value miles away if the request
          falls outside the grid. Tunable via <code>tolerance_deg</code>.
        </li>
        <li>
          <strong>Longitude convention.</strong> MRMS publishes on{" "}
          <code>[0, 360)</code>; the API speaks <code>[-180, 180]</code> on
          the wire. The translation happens server-side; you never see it.
        </li>
        <li>
          <strong>Matched coords.</strong> The response carries both the
          requested <code>lat</code>/<code>lng</code> and the actual cell
          coords the value came from — useful for caching, deduping, or
          confirming "you asked for X, you got cell Y".
        </li>
      </ul>

      <h2>Polygon reduction</h2>
      <p>
        <code>GET /v1/mrms/grids/polygon</code> applies a reducer over the
        cells of one grid whose centres fall inside a polygon. Vertices are
        flat <code>lng,lat,lng,lat,...</code> (GeoJSON / OGC ordering, same
        as <code>bbox</code>); the ring is implicitly closed. Four reducers:
      </p>
      <table>
        <thead>
          <tr>
            <th>Reducer</th>
            <th>Returns</th>
            <th>Use case</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><code>max</code></td>
            <td>Highest value among cells inside the polygon</td>
            <td>Worst-case intensity over a region</td>
          </tr>
          <tr>
            <td><code>mean</code></td>
            <td>Arithmetic mean</td>
            <td>Aggregate exposure</td>
          </tr>
          <tr>
            <td><code>min</code></td>
            <td>Lowest value</td>
            <td>"All clear" threshold checks</td>
          </tr>
          <tr>
            <td><code>count_ge</code></td>
            <td>Number of cells with value ≥ <code>threshold</code></td>
            <td>"Is anything ≥ 40 dBZ in this polygon?" — geofencing</td>
          </tr>
        </tbody>
      </table>
      <p>
        The polygon's bounding box is used to slice the grid down before the
        ray-cast mask runs, so a small region over CONUS only loads a few
        kilobytes off Zarr instead of the full ~100 MB array.
      </p>

      <h2>Nowcasts</h2>
      <p>
        For each newly-materialised observation grid, the{" "}
        <code>aeroza-nowcast-mrms</code> worker generates predicted grids at
        10, 30, and 60-minute horizons and persists them to{" "}
        <code>mrms_nowcasts</code>. The catalog surface is{" "}
        <code>GET /v1/nowcasts</code> — same shape as{" "}
        <code>/v1/mrms/grids</code> with two extra columns:
      </p>
      <ul>
        <li>
          <code>algorithm</code> — which forecaster produced this row
          (currently <code>persistence</code>; pySTEPS / NowcastNet later).
        </li>
        <li>
          <code>forecastHorizonMinutes</code> — lead time. The (algorithm,
          horizon) pair is the dimension we report verification numbers
          against.
        </li>
      </ul>
      <p>
        The v1 algorithm is <strong>persistence</strong> — every prediction
        is just the observation copied forward. That sounds trivial, and it
        is, but it's also the documented baseline (§7 of the plan): real
        nowcasting (pySTEPS, NowcastNet) wins by beating persistence at each
        horizon. Persistence forecasts running from day one means the
        verification pipeline produces real numbers from day one.
      </p>
      <p>
        Newly-persisted nowcasts also publish{" "}
        <code>aeroza.nowcast.grids.new</code> on NATS. Webhook subscriptions
        that include this event in their <code>events</code> array receive a
        signed delivery per persisted forecast.
      </p>

      <h2>Calibration — the moat</h2>
      <p>
        The <code>aeroza-verify-nowcasts</code> worker scores every
        previously-issued forecast against the real observation that arrives
        at its <code>validAt</code>. Per-(forecast, observation) MAE / bias /
        RMSE rows live in <code>nowcast_verifications</code>;{" "}
        <code>GET /v1/calibration</code> aggregates them by algorithm ×
        horizon over a window:
      </p>
      <table>
        <thead>
          <tr>
            <th>Metric</th>
            <th>Reads as</th>
            <th>What it tells you</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><code>maeMean</code></td>
            <td>Mean absolute error (dBZ)</td>
            <td>How far off, on average, ignoring direction</td>
          </tr>
          <tr>
            <td><code>biasMean</code></td>
            <td>Mean signed error (dBZ)</td>
            <td>Whether the algorithm runs hot or cold on average</td>
          </tr>
          <tr>
            <td><code>rmseMean</code></td>
            <td>Root-mean-square error (dBZ)</td>
            <td>Like MAE but penalises big misses harder</td>
          </tr>
          <tr>
            <td><code>sampleCount</code></td>
            <td>Cells contributing to the means</td>
            <td>The denominator — small numbers mean noisy aggregates</td>
          </tr>
        </tbody>
      </table>
      <p>
        Means are <strong>sample-weighted</strong>: a verification with 1M
        cells contributes 1M times to the bucket. Small windows of bad
        weather shouldn't dominate the average just because they're more
        frequent.
      </p>
      <p>
        Per the plan §3.3, calibration is the <em>trust</em> signal nobody
        else in the dev-API weather space publishes. Brier scores and
        reliability diagrams (probabilistic forecasts) land once we have an
        ensemble forecaster; for now MAE / bias / RMSE on the persistence
        baseline is honest enough to point a chart at.
      </p>

      <h2>Stats snapshot</h2>
      <p>
        <code>GET /v1/stats</code> is a compact "what does the system know
        right now?" endpoint: alert counts (active, total, latest expiry),
        MRMS file/grid counts, and the freshest <code>valid_at</code> /{" "}
        <code>materialised_at</code> timestamps. Cheap aggregate queries —
        designed to be polled every 10–30 seconds by a dashboard.
      </p>

      <hr />

      <p>
        Ready to make queries? See the <Link href="/docs/api">API reference</Link>{" "}
        or open the <Link href="/console">dev console</Link> to try them
        against live data.
      </p>
    </DocsLayout>
  );
}
