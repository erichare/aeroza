import type { Metadata } from "next";
import Link from "next/link";
import Script from "next/script";

export const metadata: Metadata = {
  title: "API explorer",
  description:
    "Interactive Aeroza v1 API explorer powered by Scalar — browse routes, " +
    "inspect schemas, and send requests against the live FastAPI surface.",
};

const API_BASE = process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000";

// Scalar's standalone bundle. Pinned to a major version so a downstream
// breaking change doesn't silently land in production via the CDN.
// https://github.com/scalar/scalar/tree/main/packages/api-reference
const SCALAR_BUNDLE_URL =
  "https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.25";

// Configuration for the embedded reference. Spec URL + a few cosmetic
// hints. Theming is *not* done via Scalar's ``customCss`` config key
// — the v1.25 standalone bundle silently drops it (we verified in
// devtools that the rule never lands in any stylesheet). Instead we
// inject our own ``<style>`` tag below.
const SCALAR_CONFIG = {
  spec: { url: `${API_BASE}/openapi.json` },
  theme: "default",
  layout: "modern",
  // The bundle ships dark-mode CSS regardless of these flags; we
  // counter the cascade in SCOPED_CHROME_RESET below. Hiding the
  // toggle is still useful: even without theming both states, we
  // don't want the user flipping a switch that cosmetically breaks.
  hideDarkModeToggle: true,
  hideClientButton: false,
  defaultHttpClient: { targetKey: "shell", clientKey: "curl" },
  // Intentionally no ``metaData`` — Scalar would otherwise overwrite
  // the document title with its own, clobbering the Next.js
  // ``metadata.title`` we export at the top of this file.
};

// Defensive override against Scalar's standalone bundle. Two
// problems the bundle creates that this CSS undoes:
//
// 1. It adds ``dark-mode`` to ``<body>``, which redefines the
//    ``--scalar-*`` colour variables to a dark palette that
//    cascades down through ``.scalar-app``. Even passing
//    ``darkMode: false`` + ``forceDarkModeState: "light"`` to the
//    config doesn't stop this on v1.25.
// 2. It writes ``html { background: rgb(15,15,15) }`` straight
//    onto the document root, so the *Aeroza* page chrome (nav,
//    breadcrumbs, page header) becomes dark-on-dark.
//
// We answer both by re-asserting the Meridian palette via
// ``body.dark-mode`` AND ``html, body``, with ``!important`` because
// the bundle's CSS lands at the same specificity and we need to win
// without playing the order-of-injection game. Scoped to this route's
// ``<head>`` so no other page sees these overrides.
const SCOPED_CHROME_RESET = `
  /* Aeroza page chrome — undo the bundle's globals on html/body. */
  html, body {
    background-color: var(--color-bg) !important;
    color: var(--color-text) !important;
    color-scheme: light !important;
  }

  /* Re-apply the Meridian palette inside the Scalar widget. We target
     both the bare .scalar-app (so a fresh-mounted widget reads our
     values) and body.dark-mode (so the bundle's own class-on-body
     override is itself overridden). */
  .scalar-app,
  body.dark-mode,
  body.dark-mode .scalar-app {
    --scalar-color-1: rgb(16 28 44) !important;
    --scalar-color-2: rgb(46 60 78) !important;
    --scalar-color-3: rgb(100 116 134) !important;
    --scalar-color-accent: rgb(174 122 51) !important;
    --scalar-background-1: rgb(247 248 250) !important;
    --scalar-background-2: rgb(232 236 240) !important;
    --scalar-background-3: rgb(220 226 234) !important;
    --scalar-background-accent: rgb(174 122 51 / 0.1) !important;
    --scalar-border-color: rgb(197 207 218) !important;
    --scalar-color-green: rgb(47 112 95) !important;
    --scalar-color-orange: rgb(196 142 47) !important;
    --scalar-color-red: rgb(168 65 64) !important;
    --scalar-color-blue: rgb(58 120 148) !important;
    --scalar-radius: 6px !important;
    --scalar-radius-lg: 8px !important;
    --scalar-font: var(--font-sans), Inter, system-ui, sans-serif !important;
    --scalar-font-code: var(--font-mono), "JetBrains Mono", monospace !important;
  }
`;

/**
 * Interactive API explorer powered by Scalar.
 *
 * Why a dedicated page (rather than embedding into /docs/api): Scalar wants
 * a wide canvas — three columns at desktop widths — and the DocsLayout's
 * narrow article column would compress it to unreadability. So this route
 * uses a thin breadcrumb header above a full-bleed embed and skips the
 * docs sidebar.
 *
 * Why standalone CDN bundle (rather than @scalar/api-reference-react): zero
 * new npm dependencies, no React 19 peer-dep risk, and the bundle can be
 * pinned via the CDN URL. The component injects itself into the
 * #api-reference script tag's parent — the empty div below is the mount
 * point's neighbour.
 */
export default function ApiExplorerPage() {
  return (
    <main className="mx-auto flex w-full max-w-[1400px] flex-col gap-4 px-6 py-8">
      <style dangerouslySetInnerHTML={{ __html: SCOPED_CHROME_RESET }} />
      <nav className="flex items-center gap-2 text-[11px] text-muted">
        <Link href="/docs" className="hover:text-text">
          Docs
        </Link>
        <span aria-hidden>›</span>
        <Link href="/docs/api" className="hover:text-text">
          API reference
        </Link>
        <span aria-hidden>›</span>
        <span className="text-text">Explorer</span>
      </nav>

      <header className="flex flex-col gap-1">
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">
          API explorer
        </h1>
        <p className="text-sm text-muted">
          Interactive client for the live{" "}
          <a href={API_BASE} target="_blank" rel="noreferrer" className="text-accent hover:underline">
            {API_BASE}
          </a>{" "}
          surface. Browse, inspect, and send requests directly from the
          page. The spec is pulled from{" "}
          <a
            href={`${API_BASE}/openapi.json`}
            target="_blank"
            rel="noreferrer"
            className="text-accent hover:underline"
          >
            /openapi.json
          </a>
          ; reload to pick up changes.
        </p>
      </header>

      <div className="rounded-lg border border-border/60 bg-surface/40 shadow-sm">
        {/* Scalar's standalone bundle is dual-configured here:

            * `data-url` is the canonical attribute the v1.25 bundle
              reads to know which spec to fetch. The JSON-body form
              (``type="application/json"``) was inconsistent in this
              version and silently dropped the fetch — leaving the
              widget rendered but empty.
            * `data-configuration` carries every other knob so we
              keep the metaData / theme / hideDarkModeToggle hints,
              and they're applied in addition to the URL above. */}
        <script
          id="api-reference"
          data-url={`${API_BASE}/openapi.json`}
          data-configuration={JSON.stringify(SCALAR_CONFIG)}
        />
        <Script src={SCALAR_BUNDLE_URL} strategy="afterInteractive" />
      </div>
    </main>
  );
}
