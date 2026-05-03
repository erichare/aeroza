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

      <h2>Raster tiles (the map layer)</h2>
      <p>
        <code>GET /v1/mrms/tiles/&#123;z&#125;/&#123;x&#125;/&#123;y&#125;.png</code>{" "}
        renders a 256×256 Web-Mercator tile of the latest matching grid:
        nearest-neighbour sample from the Zarr store, NWS dBZ ramp,
        86%-opaque so the basemap shows through where there's no echo.
        Tiles outside the grid extent (or when no grid has materialised
        yet) come back as a fully-transparent PNG so MapLibre / Leaflet
        don't spam 404 retries. <code>fileKey</code> pins a specific
        grid — used by the timeline scrubber on{" "}
        <Link href="/map">/map</Link> to fetch historical tiles. The
        same <code>fileKey</code> mechanism powers the <strong>1-hour
        radar auto-loop</strong> in <Link href="/map">/map</Link>'s
        header: the page boots playing through every grid in the last
        hour at 2× by default, with a speed selector (1×/2×/4×/8×) for
        slowing down to inspect a developing storm cell. Scrubbing the
        timeline pauses the loop; pressing <strong>▶ Loop 1h</strong>{" "}
        resumes it.
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

      <h2>METAR (surface observations)</h2>
      <p>
        METAR is the global standard for hourly surface weather reporting
        at airports. The <code>aeroza-ingest-metar</code> worker polls the
        Aviation Weather Center JSON API for a configurable list of ICAO
        stations (default: a CONUS top-20 sample) every 5 minutes. AWC
        returns already-parsed records, so there is no in-tree METAR text
        parser; the <code>rawText</code> column preserves the original
        string for callers who want their own.
      </p>
      <p>
        Each row is keyed on <code>(stationId, observationTime)</code> —
        re-fetches that find no change are no-ops, and SPECI updates
        within a cycle update the row in place. Measurement fields are
        nullable (a station whose dewpoint sensor isn't reporting still
        gets a row, just with <code>null</code> in those columns).
      </p>
      <ul>
        <li>
          <strong>List:</strong> <code>GET /v1/metar</code> — filter by{" "}
          <code>station</code>, <code>since</code>/<code>until</code>,{" "}
          <code>bbox</code> (same convention as <code>/v1/alerts</code>),
          and <code>limit</code>. Newest first.
        </li>
        <li>
          <strong>Latest:</strong>{" "}
          <code>GET /v1/metar/{`{station}`}/latest</code> — most-recent
          observation for one airport. Case-insensitive on the path.
        </li>
      </ul>
      <p>
        Useful as ground-truth point observations next to the MRMS gridded
        products: sanity-check a nowcast at a specific airport, or join
        METAR readings against forecast cells for station-resolved
        verification.
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
          <code>algorithm</code> — which forecaster produced this row.
          Two ship today: <code>persistence</code> (the §7 baseline) and{" "}
          <code>pysteps</code> (Lucas–Kanade dense optical flow + semi-
          Lagrangian extrapolation). NowcastNet / ensemble pySTEPS land
          later.
        </li>
        <li>
          <code>forecastHorizonMinutes</code> — lead time. The (algorithm,
          horizon) pair is the dimension we report verification numbers
          against.
        </li>
      </ul>
      <p>
        The two algorithms are <em>peers</em> on the calibration page —
        their MAE / bias / RMSE rows trend side-by-side. Persistence is
        the trivial copy-forward baseline; pySTEPS computes a velocity
        field from the last few observations and advects the most
        recent frame along it. Run pysteps with
        {" "}<code>aeroza-nowcast-mrms --algorithm pysteps</code>{" "}
        (the worker fetches a small lookback window per tick from the
        catalog, so there's no separate state to manage). When the
        catalog has fewer than the required past frames, pySTEPS falls
        back to persistence rather than crashing.
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
          <tr>
            <td><code>pod</code> / <code>far</code> / <code>csi</code></td>
            <td>Categorical skill scores</td>
            <td>How well the algorithm caught threshold crossings</td>
          </tr>
          <tr>
            <td><code>thresholdDbz</code></td>
            <td>The threshold the categorical metrics scored against</td>
            <td>Default 35 dBZ — operational meteorology's "convective cell" cutoff. <code>null</code> if rows in the bucket disagreed.</td>
          </tr>
        </tbody>
      </table>
      <p>
        Continuous means (<code>maeMean</code>, <code>biasMean</code>,{" "}
        <code>rmseMean</code>) are <strong>sample-weighted</strong>: a
        verification with 1M cells contributes 1M times to the bucket.
        Small windows of bad weather shouldn't dominate the average just
        because they're more frequent.
      </p>
      <p>
        Categorical scores (<code>pod</code> / <code>far</code> /{" "}
        <code>csi</code>) compute on a <em>contingency table</em> stored
        per verification — four counts of forecast/observed crossings of
        the threshold (hits, misses, false alarms, correct negatives). The
        aggregate <strong>sums</strong> the cells across rows then
        computes the ratio at the end; averaging POD/FAR/CSI across rows
        directly is wrong (the average of ratios isn't the ratio of
        averages). When a bucket has no contributing categorical rows or
        the denominator is zero, the route emits <code>null</code> rather
        than a misleading 0.
      </p>
      <p>
        For trend-watching,{" "}
        <code>GET /v1/calibration/series</code> returns the same metrics
        time-bucketed (<code>bucketSeconds</code> from 5 min to 1 day).
        That's what the sparkline on{" "}
        <Link href="/calibration">/calibration</Link> charts: same Y-axis
        per row so a row's downward trend lines up with a peer's at a
        glance. The metric switcher above the matrix has six tabs — MAE
        (continuous error), POD / FAR / CSI (categorical skill at the
        configured threshold), and <strong>Brier</strong> /{" "}
        <strong>CRPS</strong> (probabilistic skill, ensemble rows only).
        Each non-baseline cell shows a small{" "}
        <span className="font-mono">↑/↓ N% vs persistence</span> ribbon
        on the active metric so the question "did this algorithm beat
        the baseline?" answers itself at a glance.
      </p>
      <p>
        Per the plan §3.3, calibration is the <em>trust</em> signal nobody
        else in the dev-API weather space publishes. The probabilistic
        complement to POD/FAR/CSI now ships too: when the source nowcast
        is an ensemble (e.g.{" "}
        <code>--algorithm lagged-ensemble</code>), the verifier scores{" "}
        <strong>Brier</strong> (mean squared error of event probability)
        and the <strong>fair-CRPS</strong> ensemble estimator
        (continuous ranked probability score) and the calibration
        aggregate exposes <code>brierMean</code> / <code>crpsMean</code>{" "}
        / <code>ensembleSize</code> alongside MAE. Reliability diagrams
        and a STEPS-perturbed ensemble are the next probabilistic-skill
        steps.
      </p>

      <h2>Webhooks &amp; alert rules</h2>
      <p>
        Every subject the platform publishes on NATS is also a webhook
        event. Subscribers register a target URL and an{" "}
        <code>events</code> array; the dispatcher translates each NATS
        message into an HTTP POST with an HMAC-SHA256 signature in the{" "}
        <code>Aeroza-Signature</code> header (Stripe-style{" "}
        <code>v1=&lt;hex&gt;</code>) and the publish time in{" "}
        <code>Aeroza-Timestamp</code>. Two subjects are wired so far:
      </p>
      <ul>
        <li>
          <code>aeroza.alerts.nws.new</code> — every newly-observed NWS
          alert (the same stream behind <code>/v1/alerts/stream</code>).
        </li>
        <li>
          <code>aeroza.nowcast.grids.new</code> — every persisted nowcast
          (one event per algorithm × horizon × valid_at).
        </li>
      </ul>
      <p>
        Subscriptions can be filtered by an <strong>alert rule</strong> —
        a tiny DSL with two predicate kinds: <code>point</code>{" "}
        (alert polygon intersects a circle of radius{" "}
        <code>radiusMeters</code> around <code>(lat, lng)</code>) and{" "}
        <code>polygon</code> (alert intersects a caller-supplied
        polygon). Rules can also gate on a minimum severity and an
        optional event-name allowlist. One rule can back many
        subscriptions, so a "Texas storms ≥ Severe" rule is a
        first-class object you can attach to as many webhook targets as
        you need.
      </p>
      <p>
        The dispatcher's retry queue records every attempt in{" "}
        <code>webhook_deliveries</code>: response status, latency,
        response-body excerpt for failures. A circuit breaker flips a
        subscription to <code>disabled</code> after repeated
        non-success — a visible, human-readable signal so a 4xx storm
        from a flaky receiver doesn't burn the queue. CRUD all of the
        above through{" "}
        <Link href="/docs/api">/v1/webhooks</Link> and{" "}
        <Link href="/docs/api">/v1/alert-rules</Link>.
      </p>

      <h2>Stats snapshot</h2>
      <p>
        <code>GET /v1/stats</code> is a compact "what does the system know
        right now?" endpoint: alert counts (active, total, latest expiry),
        MRMS file/grid counts, and the freshest <code>valid_at</code> /{" "}
        <code>materialised_at</code> timestamps. Cheap aggregate queries —
        designed to be polled every 10–30 seconds by a dashboard.
      </p>

      <h2>API keys &amp; auth</h2>
      <p>
        Every route is anonymous by default. Bearer-token auth exists
        server-side and is opt-in per deployment via{" "}
        <code>AEROZA_AUTH_REQUIRED=true</code>. Tokens are minted with
        the <code>aeroza-api-keys</code> CLI and have the format{" "}
        <code>aza_live_&lt;random&gt;</code>; only the HMAC-SHA-256 hash
        is persisted, keyed by <code>AEROZA_API_KEY_SALT</code> for
        domain separation.
      </p>
      <p>
        Pass the token as <code>Authorization: Bearer &lt;token&gt;</code>{" "}
        (or set <code>apiKey</code> on the SDK client). Currently the only
        gated route is <code>GET /v1/me</code>, which returns the calling
        key's metadata: name, owner, prefix (visible identifier),
        scopes, rate-limit class, and last-used timestamp. HTTP CRUD
        over <code>/v1/api-keys</code> arrives once we have an admin scope
        to gate it on; until then the CLI is the management plane.
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
