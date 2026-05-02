/**
 * Lazy loader + TopoJSON→GeoJSON converter for the US state boundaries
 * served from `/us-states-10m.topojson`.
 *
 * The file is the public-domain `us-atlas@3` `states-10m.json` from
 * Mike Bostock's d3 distribution. ~115 KB on disk; fetched once per
 * page load and memoized so repeated map mounts (e.g. strict-mode
 * remount) don't re-decode it.
 *
 * We hold the result behind a Promise so concurrent callers all get the
 * same in-flight fetch instead of starting their own.
 */

import { feature as topojsonFeature } from "topojson-client";
import type { GeometryCollection, Topology } from "topojson-specification";
import type { FeatureCollection, MultiPolygon, Polygon } from "geojson";

export type StateBoundariesCollection = FeatureCollection<
  Polygon | MultiPolygon,
  { name?: string }
>;

const TOPOJSON_PATH = "/us-states-10m.topojson";
const STATES_OBJECT_NAME = "states";

let cached: Promise<StateBoundariesCollection> | null = null;

export function loadUsStateBoundaries(): Promise<StateBoundariesCollection> {
  if (cached !== null) {
    return cached;
  }
  cached = (async () => {
    const response = await fetch(TOPOJSON_PATH, { cache: "force-cache" });
    if (!response.ok) {
      cached = null;
      throw new Error(
        `failed to load ${TOPOJSON_PATH}: ${response.status} ${response.statusText}`,
      );
    }
    const topology = (await response.json()) as Topology;
    const states = topology.objects[STATES_OBJECT_NAME] as
      | GeometryCollection
      | undefined;
    if (!states) {
      throw new Error(
        `${TOPOJSON_PATH} has no '${STATES_OBJECT_NAME}' object`,
      );
    }
    return topojsonFeature(
      topology,
      states,
    ) as unknown as StateBoundariesCollection;
  })();
  return cached;
}

/** Reset the memoized load. Test-only escape hatch. */
export function _resetUsStateBoundariesCache(): void {
  cached = null;
}
