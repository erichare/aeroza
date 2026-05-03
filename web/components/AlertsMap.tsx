"use client";

import type { FeatureCollection, GeoJsonProperties, Geometry } from "geojson";
import maplibregl, {
  type GeoJSONSource,
  type Map as MapLibreMap,
  type MapMouseEvent,
} from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useRef, useState } from "react";

import {
  API_BASE,
  type AlertFeature,
  type AlertFeatureCollection,
  type Severity,
  fetchAlerts,
} from "@/lib/api";
import { loadUsStateBoundaries } from "@/lib/usStates";

import { SeverityBadge } from "./SeverityBadge";
import { StatusDot } from "./StatusDot";

const SOURCE_ID = "alerts";
const FILL_LAYER_ID = "alerts-fill";
const OUTLINE_LAYER_ID = "alerts-outline";

const RADAR_SOURCE_ID = "mrms-radar";
const RADAR_LAYER_ID = "mrms-radar";
// Insert radar layer just below the alert polygons so alerts stay on top
// (severity polygons are the primary signal; radar is the backdrop).

const STATES_SOURCE_ID = "us-states";
const STATES_LINE_LAYER_ID = "us-states-line";
// State borders sit above radar so the line is always visible, but below
// alerts so a severe-storm polygon still wins for hit-testing. The line
// colour is a desaturated prussian ink that reads on both the glacier
// basemap and through translucent radar without competing with the alert
// palette.
const STATE_BORDER_COLOR = "#1a2638";

const REFRESH_INTERVAL_MS = 30_000;
// Bust the radar tile cache once per minute so the layer trends fresh as
// new MRMS grids land. We don't reload the whole layer — appending a
// `?_=N` query string forces MapLibre to re-fetch tiles without needing
// to recreate the source.
const RADAR_REFRESH_INTERVAL_MS = 60_000;

const SEVERITY_ORDER: Severity[] = [
  "Extreme",
  "Severe",
  "Moderate",
  "Minor",
  "Unknown",
];

// Hex colors keyed off the same palette that powers SeverityBadge — kept here
// because MapLibre paint expressions can't read CSS custom properties. Tuned
// for the Meridian theme: cooler / more muted than the warm-parchment
// versions, with a single warm note (Severe) so the urgency ladder still
// reads at a glance over the glacier basemap.
const SEVERITY_FILL_COLOR: Record<Severity, string> = {
  Extreme: "#a84140",   // signal coral
  Severe: "#c4892f",    // autumn brass
  Moderate: "#3a7894",  // muted petrol
  Minor: "#3f8474",     // sea green
  Unknown: "#647686",   // cool slate
};

// CARTO's basemap raster tiles — free, attribution-required. The
// `voyager_nolabels` variant is a desaturated cool-grey base that pairs
// with the Meridian palette without competing with the radar overlay.
// MapLibre doesn't interpolate Leaflet-style `{a-c}` subdomain
// placeholders, so we list the three explicitly and let it round-robin.
const CARTO_SUBDOMAINS = ["a", "b", "c"] as const;
const cartoTiles = (style: string): string[] =>
  CARTO_SUBDOMAINS.map(
    (s) => `https://${s}.basemaps.cartocdn.com/${style}/{z}/{x}/{y}.png`,
  );

const RASTER_ATTRIBUTION =
  '© <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noreferrer">OpenStreetMap</a> contributors © <a href="https://carto.com/attributions" target="_blank" rel="noreferrer">CARTO</a>';

const STYLE: maplibregl.StyleSpecification = {
  version: 8,
  glyphs: "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
  sources: {
    "raster-base": {
      type: "raster",
      tiles: cartoTiles("voyager_nolabels"),
      tileSize: 256,
      attribution: RASTER_ATTRIBUTION,
    },
    "raster-labels": {
      type: "raster",
      tiles: cartoTiles("voyager_only_labels"),
      tileSize: 256,
    },
  },
  layers: [
    { id: "raster-base", type: "raster", source: "raster-base" },
    { id: "raster-labels", type: "raster", source: "raster-labels" },
  ],
};

interface AlertsMapProps {
  initialBounds?: [number, number, number, number]; // [west, south, east, north]
  /**
   * Filter the rendered alerts to those active at this moment in time. Uses
   * each feature's `effective`/`onset` (start) and `expires`/`ends` (end) to
   * decide. When omitted, every fetched alert is shown.
   */
  displayedAt?: Date | null;
  /**
   * Show the MRMS reflectivity raster underneath the alert polygons. Tiles
   * come from `/v1/mrms/tiles/{z}/{x}/{y}.png` (latest grid).
   */
  showRadar?: boolean;
  /**
   * Pin the radar layer to a specific MRMS grid by its source file key.
   * When set, the tile URL gets `?fileKey=...` appended and the cache-bust
   * refresh tick is suppressed (the radar is no longer "live"). Used by
   * /demo to scrub the radar through historical grids.
   */
  radarFileKey?: string | null;
  /**
   * Optional pre-warm hint: the next radar fileKey the caller is about
   * to swap to. When set, the map fires off ``new Image()`` requests
   * for every visible tile under that key while the *current* frame
   * is still on screen — by the time the loop advances, MapLibre's
   * cache holds the bytes and the swap is instant. Pinned tiles are
   * forever-immutable on the server (``Cache-Control: immutable``),
   * so the prefetch is a one-time cost per (fileKey, viewport).
   */
  prefetchNextFileKey?: string | null;
  /**
   * Hide the in-map severity / dBZ legend overlay. Used by the landing-page
   * hero embed where the legend sits in a sibling panel and would otherwise
   * crowd the smaller viewport.
   */
  hideLegend?: boolean;
  onLoaded?: (collection: AlertFeatureCollection) => void;
}

const DEFAULT_BOUNDS: [number, number, number, number] = [-125, 24, -66, 50];

/**
 * Was the alert active at `asOf`?
 *
 * Start = `onset` || `effective` (earliest known begin time).
 * End   = `ends` || `expires` (latest known finish time).
 *
 * Missing-start means "always started before now"; missing-end means
 * "open-ended". Both falsy → the alert is treated as currently in force at
 * any time, which matches /v1/alerts's "currently active" filter.
 */
function wasActiveAt(feature: AlertFeature, asOf: Date): boolean {
  const t = asOf.getTime();
  const p = feature.properties;
  const startStr = p.onset ?? p.effective;
  const endStr = p.ends ?? p.expires;
  const startMs = startStr ? Date.parse(startStr) : Number.NEGATIVE_INFINITY;
  const endMs = endStr ? Date.parse(endStr) : Number.POSITIVE_INFINITY;
  return t >= startMs && t <= endMs;
}

export function AlertsMap({
  initialBounds,
  displayedAt,
  showRadar = true,
  radarFileKey = null,
  prefetchNextFileKey = null,
  hideLegend = false,
  onLoaded,
}: AlertsMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  const [selected, setSelected] = useState<AlertFeature | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [ready, setReady] = useState(false);
  // Cache the most recent fetch so re-filtering on `displayedAt` change
  // doesn't trigger a network round trip.
  const lastCollectionRef = useRef<AlertFeatureCollection | null>(null);

  // Build the map exactly once. Strict mode's mount→cleanup→remount within
  // the same tick wrecks MapLibre's WebGL context, so we defer the actual
  // construction to a microtask — by the time it runs, the strict-mode
  // double-invocation has settled and we only ever create one map.
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    let cancelled = false;
    let createdMap: MapLibreMap | null = null;

    const timer = setTimeout(() => {
      if (cancelled || !containerRef.current || mapRef.current) return;
      const map = new maplibregl.Map({
        container: containerRef.current,
        style: STYLE,
        bounds: initialBounds ?? DEFAULT_BOUNDS,
        fitBoundsOptions: { padding: 40, animate: false },
        attributionControl: { compact: true },
      });
      mapRef.current = map;
      createdMap = map;
      map.addControl(
        new maplibregl.NavigationControl({ showCompass: false }),
        "top-right",
      );
      map.on("load", onMapLoad);
    }, 0);

    function onMapLoad() {
      const map = mapRef.current;
      if (!map) return;

      // Radar source/layer — sits below alert polygons so a severe-storm
      // alert always paints on top of the reflectivity blob it covers.
      // Cache-bust query (`?_=ts`) is what we mutate on the refresh tick
      // to fetch fresher tiles without recreating the source. When
      // `radarFileKey` is set, we pin to a specific historical grid via
      // the `fileKey` query param and the refresh tick becomes a no-op.
      map.addSource(RADAR_SOURCE_ID, {
        type: "raster",
        tiles: [buildRadarTileUrl(radarFileKey)],
        tileSize: 256,
        attribution: "MRMS · NOAA / NSSL",
      });
      map.addLayer({
        id: RADAR_LAYER_ID,
        type: "raster",
        source: RADAR_SOURCE_ID,
        layout: { visibility: showRadar ? "visible" : "none" },
        paint: {
          // Slightly more transparent than before so the new state-border
          // layer reads through light precip without losing the storm's
          // signal in the dense core.
          "raster-opacity": 0.78,
          // Linear instead of nearest at the GL stage smooths the
          // remaining cell→pixel boundaries the server bilinear sampler
          // doesn't reach (e.g. when MapLibre is upscaling between zoom
          // levels mid-pinch).
          "raster-resampling": "linear",
          // Fade duration is a function of mode. In live mode (no
          // radarFileKey) we fade quickly because each tile-URL swap is a
          // refresh, not a logical step in time. In pinned mode (the
          // /demo replay) we fade much longer so the eye reads
          // consecutive frames as continuous motion rather than discrete
          // jumps. 700ms was chosen against the 800ms-hold of the slowest
          // playback speed — the fade fits within the hold but only
          // barely, so the next frame is already ramping in by the time
          // the previous fully clears.
          "raster-fade-duration": radarFileKey === null ? 200 : 700,
        },
      });

      // US state boundaries — fetched once, shared across map instances
      // via the in-module promise cache. We add the source and layer
      // immediately with an empty FeatureCollection placeholder, then
      // swap in the real data when the fetch resolves. That avoids a
      // race between map.addLayer() and a slow asset response on a cold
      // CDN.
      map.addSource(STATES_SOURCE_ID, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      map.addLayer({
        id: STATES_LINE_LAYER_ID,
        type: "line",
        source: STATES_SOURCE_ID,
        paint: {
          "line-color": STATE_BORDER_COLOR,
          // Width grows gently with zoom — too thick at z=4 covers
          // small states; too thin at z=8 disappears against echoes.
          "line-width": [
            "interpolate",
            ["linear"],
            ["zoom"],
            3, 0.45,
            5, 0.8,
            8, 1.2,
            10, 1.6,
          ],
          "line-opacity": 0.55,
        },
      });
      void loadUsStateBoundaries()
        .then((collection) => {
          const source = map.getSource(STATES_SOURCE_ID) as
            | GeoJSONSource
            | undefined;
          if (!source) return;
          source.setData(
            collection as unknown as FeatureCollection<Geometry, GeoJsonProperties>,
          );
        })
        .catch((err: unknown) => {
          // A missing/bad state-borders file shouldn't break the map —
          // log to the console for debugging and continue without
          // borders. The radar + alerts layers carry the rest of the UX.
          // eslint-disable-next-line no-console
          console.warn("us-states.load_failed", err);
        });

      map.addSource(SOURCE_ID, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      map.addLayer({
        id: FILL_LAYER_ID,
        type: "fill",
        source: SOURCE_ID,
        paint: {
          "fill-color": [
            "match",
            ["get", "severity"],
            "Extreme", SEVERITY_FILL_COLOR.Extreme,
            "Severe", SEVERITY_FILL_COLOR.Severe,
            "Moderate", SEVERITY_FILL_COLOR.Moderate,
            "Minor", SEVERITY_FILL_COLOR.Minor,
            SEVERITY_FILL_COLOR.Unknown,
          ],
          "fill-opacity": [
            "case",
            ["boolean", ["feature-state", "hover"], false],
            0.6,
            0.4,
          ],
        },
        // Push severe events on top so a Minor doesn't obscure an Extreme.
      });
      map.addLayer({
        id: OUTLINE_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        paint: {
          "line-color": [
            "match",
            ["get", "severity"],
            "Extreme", SEVERITY_FILL_COLOR.Extreme,
            "Severe", SEVERITY_FILL_COLOR.Severe,
            "Moderate", SEVERITY_FILL_COLOR.Moderate,
            "Minor", SEVERITY_FILL_COLOR.Minor,
            SEVERITY_FILL_COLOR.Unknown,
          ],
          "line-width": 1.4,
          "line-opacity": 0.9,
        },
      });

      const handleClick = (e: MapMouseEvent) => {
        const features = map.queryRenderedFeatures(e.point, {
          layers: [FILL_LAYER_ID],
        });
        if (features.length === 0) {
          setSelected(null);
          return;
        }
        const f = features[0];
        // queryRenderedFeatures strips Polygon/MultiPolygon arrays into a JSON
        // string under feature.properties — but our props are flat scalars,
        // so we coerce back to the typed shape used by the side panel.
        setSelected({
          type: "Feature",
          geometry: f.geometry as AlertFeature["geometry"],
          properties: f.properties as unknown as AlertFeature["properties"],
        });
      };
      map.on("click", FILL_LAYER_ID, handleClick);
      map.on("mouseenter", FILL_LAYER_ID, () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", FILL_LAYER_ID, () => {
        map.getCanvas().style.cursor = "";
      });

      setReady(true);
    }

    return () => {
      cancelled = true;
      clearTimeout(timer);
      if (createdMap && mapRef.current === createdMap) {
        createdMap.remove();
        mapRef.current = null;
      }
    };
  }, [initialBounds]);

  // Poll /v1/alerts on a timer; replace the GeoJSON source data each tick.
  useEffect(() => {
    if (!ready) return;
    let cancelled = false;

    const refresh = async () => {
      try {
        const data = await fetchAlerts({ limit: 200 });
        if (cancelled) return;
        lastCollectionRef.current = data;
        applyToSource(data, displayedAt ?? null);
        setError(null);
        onLoaded?.(data);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load alerts");
        }
      }
    };

    void refresh();
    const interval = setInterval(refresh, REFRESH_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
    // `displayedAt` is intentionally omitted — the dedicated effect below
    // re-filters from cache without a network round trip.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ready, onLoaded]);

  // Re-apply the filter whenever the scrubbed time changes, using the cached
  // last collection. No re-fetch needed.
  useEffect(() => {
    if (!ready) return;
    const cached = lastCollectionRef.current;
    if (!cached) return;
    applyToSource(cached, displayedAt ?? null);
  }, [ready, displayedAt]);

  // Toggle radar visibility without rebuilding the source.
  useEffect(() => {
    if (!ready || !mapRef.current) return;
    const map = mapRef.current;
    if (!map.getLayer(RADAR_LAYER_ID)) return;
    map.setLayoutProperty(
      RADAR_LAYER_ID,
      "visibility",
      showRadar ? "visible" : "none",
    );
  }, [ready, showRadar]);

  // Radar tile URL management.
  //
  // - "Live" mode (radarFileKey === null): periodically swap the tile URL
  //   with a fresh cache-buster so MapLibre re-fetches and the user sees
  //   newer grids land as they materialise.
  // - "Pinned" mode (radarFileKey set): swap the tile URL whenever the
  //   prop changes. Used by /demo's autoplay scrubber to step the radar
  //   through historical grids one fileKey at a time. No interval timer
  //   in this mode — a refresh would just re-fetch the same image.
  useEffect(() => {
    if (!ready || !showRadar) return;
    const swap = () => {
      const map = mapRef.current;
      if (!map) return;
      const source = map.getSource(RADAR_SOURCE_ID);
      if (!source) return;
      // setTiles() is the documented way to swap a raster source's URL
      // template without losing the layer's place in the stack.
      // Casting via unknown — MapLibre's runtime exposes setTiles on
      // raster sources but the static .d.ts types it as part of an
      // internal interface only.
      (source as unknown as { setTiles?: (tiles: string[]) => void }).setTiles?.(
        [buildRadarTileUrl(radarFileKey)],
      );
    };
    swap();
    if (radarFileKey !== null) {
      // Pinned: nothing to refresh; swap once and stop.
      return;
    }
    const id = setInterval(swap, RADAR_REFRESH_INTERVAL_MS);
    return () => clearInterval(id);
  }, [ready, showRadar, radarFileKey]);

  // Prefetch the next loop frame's tiles into the browser cache so the
  // swap to that frame doesn't trigger a network round-trip. Pinned
  // tiles are forever-immutable on the server (Cache-Control:
  // immutable + a server-side LRU), so each (fileKey, viewport) only
  // gets prefetched once. We deliberately do this in an effect keyed
  // on ``prefetchNextFileKey`` rather than inside the swap loop —
  // separating "show this frame" from "warm the next" keeps either
  // path debuggable in isolation.
  useEffect(() => {
    if (!ready || !showRadar) return;
    if (!prefetchNextFileKey) return;
    if (prefetchNextFileKey === radarFileKey) return; // already on screen
    const map = mapRef.current;
    if (!map) return;
    const tiles = visibleTileCoords(map);
    if (tiles.length === 0) return;
    const urlTemplate = buildRadarTileUrl(prefetchNextFileKey);
    // Track Image refs locally so cleanup can null their .src — that
    // cancels in-flight fetches in modern browsers (per HTML spec) and
    // lets the ones that already finished get GC'd.
    const requested: HTMLImageElement[] = [];
    for (const { z, x, y } of tiles) {
      const url = urlTemplate
        .replace("{z}", String(z))
        .replace("{x}", String(x))
        .replace("{y}", String(y));
      const img = new Image();
      img.decoding = "async";
      img.src = url;
      requested.push(img);
    }
    return () => {
      // Aborts any still-loading prefetches when the loop advances or
      // the user pauses. Browser cache keeps already-fetched tiles.
      for (const img of requested) img.src = "";
    };
  }, [ready, showRadar, prefetchNextFileKey, radarFileKey]);

  function applyToSource(
    collection: AlertFeatureCollection,
    asOf: Date | null,
  ): void {
    if (!mapRef.current) return;
    const source = mapRef.current.getSource(SOURCE_ID) as GeoJSONSource | undefined;
    if (!source) return;
    const features = collection.features.filter((f) => {
      if (f.geometry === null) return false;
      if (!asOf) return true;
      return wasActiveAt(f, asOf);
    });
    // Cast via unknown because the SDK's `coordinates: unknown` is
    // intentionally looser than the geojson lib's recursive number-array
    // types — MapLibre accepts the shape at runtime regardless.
    const filtered = {
      type: "FeatureCollection" as const,
      features,
    } as unknown as FeatureCollection<Geometry, GeoJsonProperties>;
    source.setData(filtered);
  }

  return (
    <div className="relative h-full w-full">
      <div
        ref={containerRef}
        style={{ width: "100%", height: "100%" }}
      />
      {hideLegend ? null : <Legend showRadar={showRadar} />}
      {error ? (
        <div className="absolute bottom-3 left-3 rounded-md border border-warning/40 bg-bg/85 px-3 py-2 text-xs text-warning shadow-lg backdrop-blur">
          <StatusDot tone="warning" label={error} />
        </div>
      ) : null}
      {selected ? (
        <AlertDetailCard
          feature={selected}
          onClose={() => setSelected(null)}
        />
      ) : null}
    </div>
  );
}

// Pinned to the same dBZ stops as `aeroza/tiles/colormap.py` so the on-map
// legend tells the truth about what the user is looking at. Two stops
// per row keeps the strip readable at 12rem wide.
const DBZ_LEGEND_STOPS: ReadonlyArray<{ dbz: number; color: string }> = [
  { dbz: 5, color: "#04e9e7" },
  { dbz: 15, color: "#0300f4" },
  { dbz: 25, color: "#01c501" },
  { dbz: 35, color: "#fdf802" },
  { dbz: 45, color: "#fd9500" },
  { dbz: 55, color: "#d40000" },
  { dbz: 65, color: "#f800fd" },
];

function Legend({ showRadar }: { showRadar: boolean }) {
  return (
    <div className="absolute bottom-3 right-3 z-10 flex flex-col gap-2 rounded-lg border border-border/60 bg-bg/85 px-3 py-2 text-[11px] shadow-lg backdrop-blur">
      <div>
        <div className="mb-1 font-mono text-[10px] uppercase tracking-wider text-muted">
          Severity
        </div>
        <ul className="flex flex-col gap-1">
          {SEVERITY_ORDER.map((s) => (
            <li key={s} className="flex items-center gap-2 text-text">
              <span
                className="inline-block h-2.5 w-3.5 rounded-sm"
                style={{ background: SEVERITY_FILL_COLOR[s], opacity: 0.55 }}
              />
              <span>{s}</span>
            </li>
          ))}
        </ul>
      </div>
      {showRadar ? <DbzRampLegend /> : null}
    </div>
  );
}

function DbzRampLegend() {
  // CSS gradient mirrors the discrete stops used by the server-side
  // colormap. Linear-RGB blending here matches the renderer's behaviour
  // closely enough for a legend; the on-map echoes are the source of
  // truth for visible colour.
  const gradient = `linear-gradient(to right, ${DBZ_LEGEND_STOPS.map(
    (s, i) =>
      `${s.color} ${(i / (DBZ_LEGEND_STOPS.length - 1)) * 100}%`,
  ).join(", ")})`;
  return (
    <div className="border-t border-border/40 pt-2">
      <div
        className="mb-1 flex items-center gap-1 font-mono text-[10px] uppercase tracking-wider text-muted"
        title={
          // Inline tooltip — most viewers know dBZ but a few don't, and a
          // one-line explainer beats them having to leave the map. The
          // 35 dBZ anchor connects this legend to the calibration
          // panel's POD/FAR/CSI threshold so the two surfaces visibly
          // talk about the same number.
          "Decibel-relative-to-Z. Logarithmic radar reflectivity, integrating drop size and rate. " +
          "Light rain ~20 · moderate ~30 · 35+ is operational meteorology's 'convective cell' cutoff (matches /calibration's threshold) · 50+ likely large hail or intense storms."
        }
      >
        <span>Reflectivity (dBZ)</span>
        <span aria-hidden className="rounded-full border border-border/60 px-1 text-[8px] leading-tight text-muted/70">
          ?
        </span>
      </div>
      <div
        className="h-2 w-44 rounded-sm"
        style={{ background: gradient, opacity: 0.85 }}
        aria-hidden
      />
      <div className="mt-1 flex justify-between font-mono text-[9px] text-muted">
        <span>5</span>
        <span>25</span>
        <span title="Operational 'convective cell' cutoff — the threshold the /calibration page scores POD/FAR/CSI at by default">
          35
        </span>
        <span>50</span>
        <span>65+</span>
      </div>
    </div>
  );
}

function AlertDetailCard({
  feature,
  onClose,
}: {
  feature: AlertFeature;
  onClose: () => void;
}) {
  const p = feature.properties;
  return (
    <aside className="absolute right-3 top-3 z-10 max-h-[calc(100%-1.5rem)] w-[22rem] overflow-y-auto rounded-xl border border-border/60 bg-bg/90 p-4 shadow-2xl backdrop-blur">
      <div className="mb-2 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <SeverityBadge severity={p.severity} />
            <h3 className="truncate text-sm font-semibold text-text">{p.event}</h3>
          </div>
          <p className="mt-0.5 truncate font-mono text-[10px] text-muted">
            {p.senderName ?? "—"}
          </p>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="rounded-md border border-border/60 px-1.5 py-0.5 text-xs text-muted hover:border-accent/50 hover:text-accent"
          aria-label="Close detail"
        >
          ✕
        </button>
      </div>

      {p.headline ? (
        <p className="mb-3 text-xs leading-relaxed text-text/90">{p.headline}</p>
      ) : null}

      <dl className="grid grid-cols-[7rem_1fr] gap-x-2 gap-y-1.5 text-[11px]">
        <DetailRow label="Urgency" value={p.urgency} />
        <DetailRow label="Certainty" value={p.certainty} />
        <DetailRow label="Areas" value={p.areaDesc} />
        <DetailRow
          label="Effective"
          value={p.effective ? new Date(p.effective).toLocaleString() : "—"}
        />
        <DetailRow
          label="Onset"
          value={p.onset ? new Date(p.onset).toLocaleString() : "—"}
        />
        <DetailRow
          label="Expires"
          value={p.expires ? new Date(p.expires).toLocaleString() : "—"}
        />
        <DetailRow
          label="Ends"
          value={p.ends ? new Date(p.ends).toLocaleString() : "—"}
        />
      </dl>

      <p className="mt-3 break-all font-mono text-[9px] text-muted/60">{p.id}</p>
    </aside>
  );
}

function DetailRow({ label, value }: { label: string; value: string | null | undefined }) {
  return (
    <>
      <dt className="font-mono uppercase tracking-wide text-muted/80">{label}</dt>
      <dd className="text-text">{value ?? "—"}</dd>
    </>
  );
}

/**
 * Build the radar tile URL template for the MapLibre raster source.
 *
 * Live mode (no fileKey): cache-busts on every call so MapLibre re-fetches
 * tiles when we swap the URL — surfacing freshly-materialised grids.
 *
 * Pinned mode (fileKey set): asks the server for the specific grid's
 * tiles. The fileKey is encoded with `encodeURIComponent` because real
 * MRMS keys carry slashes (`CONUS/.../MergedReflectivityComposite_...`)
 * and the route's query parser treats them as opaque.
 */
function buildRadarTileUrl(fileKey: string | null): string {
  const base = `${API_BASE}/v1/mrms/tiles/{z}/{x}/{y}.png`;
  if (fileKey === null) {
    return `${base}?_=${Date.now()}`;
  }
  return `${base}?fileKey=${encodeURIComponent(fileKey)}`;
}

interface TileCoord {
  z: number;
  x: number;
  y: number;
}

/**
 * Compute the slippy-tile coords currently inside the map's viewport.
 *
 * Standard XYZ tile math: ``2^z`` total tiles per axis at zoom ``z``;
 * x is uniform in longitude, y is the Mercator latitude formula.
 * MapLibre's ``getZoom()`` returns a fractional zoom — we round to
 * the integer level the tile pyramid actually serves, which is what
 * the visible raster source consumes.
 *
 * The bounds intersect is conservative — for tiles partially in
 * view we still prefetch them so a tiny pan during the loop doesn't
 * flash an unprefetched edge.
 */
function visibleTileCoords(map: MapLibreMap): TileCoord[] {
  const z = Math.round(map.getZoom());
  if (z < 0) return [];
  const bounds = map.getBounds();
  const n = 1 << z;

  const lonToX = (lon: number): number =>
    Math.floor(((lon + 180) / 360) * n);
  const latToY = (lat: number): number => {
    const clamped = Math.max(-85.0511, Math.min(85.0511, lat));
    const rad = (clamped * Math.PI) / 180;
    return Math.floor(
      ((1 - Math.log(Math.tan(rad) + 1 / Math.cos(rad)) / Math.PI) / 2) * n,
    );
  };

  const west = bounds.getWest();
  const east = bounds.getEast();
  const south = bounds.getSouth();
  const north = bounds.getNorth();

  const xMin = Math.max(0, Math.min(n - 1, lonToX(west)));
  const xMax = Math.max(0, Math.min(n - 1, lonToX(east)));
  // Mercator y grows southward; north is the smaller index.
  const yMin = Math.max(0, Math.min(n - 1, latToY(north)));
  const yMax = Math.max(0, Math.min(n - 1, latToY(south)));

  const coords: TileCoord[] = [];
  for (let x = xMin; x <= xMax; x += 1) {
    for (let y = yMin; y <= yMax; y += 1) {
      coords.push({ z, x, y });
    }
  }
  return coords;
}
