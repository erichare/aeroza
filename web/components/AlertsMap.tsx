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
  type AlertFeature,
  type AlertFeatureCollection,
  type Severity,
  fetchAlerts,
} from "@/lib/api";

import { SeverityBadge } from "./SeverityBadge";
import { StatusDot } from "./StatusDot";

const SOURCE_ID = "alerts";
const FILL_LAYER_ID = "alerts-fill";
const OUTLINE_LAYER_ID = "alerts-outline";

const REFRESH_INTERVAL_MS = 30_000;

const SEVERITY_ORDER: Severity[] = [
  "Extreme",
  "Severe",
  "Moderate",
  "Minor",
  "Unknown",
];

// Hex colors keyed off the same palette that powers SeverityBadge — kept here
// because MapLibre paint expressions can't read CSS custom properties.
const SEVERITY_FILL_COLOR: Record<Severity, string> = {
  Extreme: "#f87171",
  Severe: "#fbbf24",
  Moderate: "#38bdfa",
  Minor: "#34d399",
  Unknown: "#94a3b8",
};

// CARTO's basemap raster tiles — free, attribution-required, dark theme that
// matches the console palette. MapLibre doesn't interpolate Leaflet-style
// `{a-c}` subdomain placeholders, so we list the three subdomains explicitly
// and let MapLibre round-robin across them.
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
      tiles: cartoTiles("dark_nolabels"),
      tileSize: 256,
      attribution: RASTER_ATTRIBUTION,
    },
    "raster-labels": {
      type: "raster",
      tiles: cartoTiles("dark_only_labels"),
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
            0.55,
            0.3,
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
      <Legend />
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

function Legend() {
  return (
    <div className="absolute bottom-3 right-3 z-10 rounded-lg border border-border/60 bg-bg/85 px-3 py-2 text-[11px] shadow-lg backdrop-blur">
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
