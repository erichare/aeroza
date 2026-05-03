/**
 * Aeroza brand mark — a stylised supercell hook echo.
 *
 * Three exports:
 *
 * - {@link AerozaGlyph} — the icon-only mark. Recognisable at 16×16
 *   (favicon) up to header sizes. Uses `currentColor` so the caller
 *   controls the colour via Tailwind text-* utilities.
 *
 * - {@link AerozaWordmark} — the glyph plus the "AEROZA" wordmark in
 *   tracked-out mono. Used in the SiteNav and in any header-style
 *   placement that wants both pieces.
 *
 * - {@link AerozaGlyphSvgString} — the same glyph rendered to a static
 *   SVG string. Used by `web/app/icon.svg` so the favicon doesn't have
 *   to import React.
 *
 * **Design notes** (so future redesigns don't have to re-derive this).
 * The hook echo is the canonical radar signature of a tornadic supercell
 * — a kidney-shaped mass of intense reflectivity with a small comma-like
 * protrusion on the south side where the mesocyclone has wrapped
 * precipitation around it. We translate that into two shapes:
 *
 *   1. A filled, slightly off-axis ellipse (the storm core / meso).
 *   2. A second smaller filled blob, offset down-right (the hook tip).
 *   3. A connecting arm that tapers between them.
 *
 * The whole thing is a single filled path so it stays sharp at favicon
 * sizes — strokes get visually inconsistent below ~24px because of
 * subpixel rendering. The off-axis tilt suggests motion (storms move
 * SW→NE in CONUS) and breaks the otherwise-symmetrical silhouette.
 */

interface AerozaGlyphProps {
  /** Square pixel size. Defaults to 24 — comfortable header size. */
  size?: number;
  /** Optional className passed through (e.g. text-accent for colour). */
  className?: string;
  /** ARIA label for the SVG. Pass null to leave it decorative. */
  title?: string | null;
}

/**
 * The path data for the hook-echo silhouette. Extracted so the same
 * geometry powers both the React component and the static favicon.
 *
 * Coordinates are in a 64×64 viewBox. The shape is a single closed
 * path: storm core (top-left filled blob) → tapered arm → hook tip
 * (bottom-right blob) → curl back into the core. Drawn with cubic
 * Bézier curves so the silhouette stays smooth at any zoom.
 */
export const AEROZA_GLYPH_PATH: string = [
  // Start at the top of the storm core (north edge, slightly east of
  // centre to suggest a NE-trending storm).
  "M 32 8",
  // Sweep counter-clockwise around the west and south sides of the
  // core, coming around to the bottom-right where the hook will start.
  "C 18 8, 8 20, 8 30",
  "C 8 42, 18 50, 28 50",
  // Transition into the hook arm — pull the silhouette down and to the
  // right, narrowing as it goes (the radar cell stretches outward).
  "C 34 50, 38 52, 42 54",
  // The hook tip — a small bulb at the bottom-right.
  "C 50 56, 56 52, 56 46",
  "C 56 40, 50 36, 44 38",
  // Curl back toward the core, completing the comma shape.
  "C 40 39, 38 42, 38 46",
  // Inside curve closing back into the south edge of the core.
  "C 38 36, 34 34, 28 34",
  "C 22 34, 18 30, 18 26",
  "C 18 18, 24 14, 32 14",
  // Continue around the top edge of the core back to the start.
  "C 38 14, 44 18, 46 24",
  "C 47 27, 50 28, 52 27",
  "C 50 17, 42 8, 32 8",
  "Z",
].join(" ");

export function AerozaGlyph({
  size = 24,
  className,
  title = "Aeroza",
}: AerozaGlyphProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 64 64"
      fill="currentColor"
      xmlns="http://www.w3.org/2000/svg"
      role={title ? "img" : "presentation"}
      aria-label={title ?? undefined}
      className={className}
    >
      {title ? <title>{title}</title> : null}
      <path d={AEROZA_GLYPH_PATH} />
    </svg>
  );
}

interface AerozaWordmarkProps {
  /** Glyph pixel size (the wordmark scales relative to it). */
  glyphSize?: number;
  /** Wrapper className — typically `text-text` or `text-accent`. */
  className?: string;
  /** Hide the "AEROZA" text. Useful for icon-only contexts. */
  textHidden?: boolean;
}

export function AerozaWordmark({
  glyphSize = 18,
  className,
  textHidden = false,
}: AerozaWordmarkProps) {
  return (
    <span className={["inline-flex items-center gap-2", className].join(" ")}>
      <AerozaGlyph size={glyphSize} title={null} />
      {textHidden ? null : (
        <span className="font-mono text-[11px] uppercase tracking-[0.2em]">
          Aeroza
        </span>
      )}
    </span>
  );
}

/**
 * Standalone SVG string — used by the static favicon. We keep it as a
 * string export rather than reaching for ReactDOMServer because it
 * lives outside any React tree (Next.js renders `app/icon.svg` itself).
 */
export const AerozaGlyphSvgString: string =
  `<?xml version="1.0" encoding="UTF-8"?>` +
  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">` +
  // Hardcoded Meridian brass — favicons can't read CSS variables.
  `<path fill="#ae7a33" d="${AEROZA_GLYPH_PATH}" />` +
  `</svg>`;
