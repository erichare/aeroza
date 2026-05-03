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

// Configuration for the embedded reference. Themed to match the Meridian
// palette (pale glacier base + prussian-ink text + aged-brass accent) so
// the embed reads as part of Aeroza, not a third-party widget.
//
// Scalar's CSS variables are namespaced --scalar-*. We only override the
// colours; spacing / typography defaults are good. The vars are written
// with raw rgb() values so they pick up our own design tokens 1:1.
const SCALAR_CONFIG = {
  spec: { url: `${API_BASE}/openapi.json` },
  theme: "default",
  layout: "modern",
  hideDarkModeToggle: false,
  hideClientButton: false,
  defaultHttpClient: { targetKey: "shell", clientKey: "curl" },
  metaData: {
    title: "Aeroza v1 API",
    description: "Programmable weather intelligence — alerts, MRMS, METAR, nowcasts.",
  },
  customCss: `
    .scalar-app {
      --scalar-color-1: rgb(16 28 44);
      --scalar-color-2: rgb(46 60 78);
      --scalar-color-3: rgb(100 116 134);
      --scalar-color-accent: rgb(174 122 51);
      --scalar-background-1: rgb(247 248 250);
      --scalar-background-2: rgb(232 236 240);
      --scalar-background-3: rgb(220 226 234);
      --scalar-background-accent: rgb(174 122 51 / 0.1);
      --scalar-border-color: rgb(197 207 218);
      --scalar-color-green: rgb(47 112 95);
      --scalar-color-orange: rgb(196 142 47);
      --scalar-color-red: rgb(168 65 64);
      --scalar-color-blue: rgb(58 120 148);
      --scalar-radius: 6px;
      --scalar-radius-lg: 8px;
      --scalar-font: var(--font-sans), Inter, system-ui, sans-serif;
      --scalar-font-code: var(--font-mono), "JetBrains Mono", monospace;
    }
  `,
};

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
        <script
          id="api-reference"
          type="application/json"
          // Scalar reads its config from the JSON body of the #api-reference
          // script tag. dangerouslySetInnerHTML is the React-safe way to
          // emit raw JSON without it being parsed as JSX children.
          dangerouslySetInnerHTML={{ __html: JSON.stringify(SCALAR_CONFIG) }}
        />
        <Script src={SCALAR_BUNDLE_URL} strategy="afterInteractive" />
      </div>
    </main>
  );
}
