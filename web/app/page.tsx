import Link from "next/link";

import { AerozaGlyph } from "@/components/AerozaLogo";
import { HeroLiveMap } from "@/components/HeroLiveMap";
import { HeroVerificationCard } from "@/components/HeroVerificationCard";

const HERO_TAGLINE = "Weather, but queryable.";
const HERO_SUBHEAD =
  "Real-time radar, predictive nowcasting with calibrated confidence, and " +
  "geospatial queries — for applications that need to understand and react " +
  "to weather in real time. Every forecast scored against reality, in public.";

interface Feature {
  title: string;
  body: string;
  endpoint: string | null;
  status: "ready" | "soon";
}

const FEATURES: ReadonlyArray<Feature> = [
  {
    title: "Real-time radar & alerts",
    body:
      "MRMS reflectivity grids materialised in seconds, NWS alerts streamed " +
      "as Server-Sent Events, and a Web-Mercator tile route ready to drop " +
      "into MapLibre or Leaflet.",
    endpoint: "GET /v1/mrms/tiles/{z}/{x}/{y}.png",
    status: "ready",
  },
  {
    title: "Geospatial queries",
    body:
      "Sample a point, reduce a polygon (max / mean / min / count above a " +
      "threshold). Lat-lng in, value out — no GIS toolchain required.",
    endpoint: "GET /v1/mrms/grids/polygon",
    status: "ready",
  },
  {
    title: "Calibrated nowcasting",
    body:
      "Predicted reflectivity 10 / 30 / 60 minutes out, scored against the " +
      "matching observation as soon as truth lands. Public MAE / bias / RMSE " +
      "per algorithm × horizon — persistence baseline today, pySTEPS / " +
      "NowcastNet next.",
    endpoint: "GET /v1/calibration",
    status: "ready",
  },
];

const CALLOUTS: ReadonlyArray<{ label: string; href: string; primary?: boolean }> = [
  { label: "View live map", href: "/map", primary: true },
  { label: "Open the dev console", href: "/console" },
  { label: "Read the docs", href: "/docs" },
  { label: "OpenAPI schema", href: "/openapi.json" },
];

export default function LandingPage() {
  return (
    <main className="mx-auto flex min-h-[calc(100vh-3rem)] w-full max-w-[1400px] flex-col gap-16 px-6 py-12">
      <Pitch />
      <LiveDemo />
      <Features />
      <BottomCta />
      <Footer />
    </main>
  );
}

/**
 * The text pitch + CTAs, sits above the fold. The brass hook-echo
 * glyph anchors the brand at logo-scale (44px) and reinforces the
 * weather-instrument register the wordmark in SiteNav establishes at
 * 16px. Headline → subhead → CTAs flows the way a deck slide reads.
 */
function Pitch() {
  return (
    <section className="flex flex-col items-start gap-5">
      <AerozaGlyph
        size={44}
        title="Aeroza"
        className="text-accent"
      />
      <h1 className="max-w-3xl font-display text-4xl font-semibold tracking-tight text-text sm:text-5xl">
        {HERO_TAGLINE}
      </h1>
      <p className="max-w-2xl text-base leading-relaxed text-muted">
        {HERO_SUBHEAD}
      </p>
      <div className="mt-1 flex flex-wrap items-center gap-3">
        {CALLOUTS.map((cta) => (
          <CallToAction key={cta.href} {...cta} />
        ))}
      </div>
    </section>
  );
}

/**
 * The live demo strip: embedded AlertsMap on the left + "were we right?"
 * calibration card on the right. Same content as the original above-the-
 * fold hero — moved below the Pitch so visitors get the framing first
 * (what is this?) and then the proof (look, it's running). The 60/40
 * split keeps the map visually dominant within this section while the
 * card balances it with the verification story.
 */
function LiveDemo() {
  return (
    <section className="grid grid-cols-1 gap-5 lg:grid-cols-[3fr_2fr]">
      <HeroLiveMap />
      <HeroVerificationCard />
    </section>
  );
}

function CallToAction({
  label,
  href,
  primary,
}: {
  label: string;
  href: string;
  primary?: boolean;
}) {
  const className = primary
    ? "rounded-md border border-accent bg-accent/15 px-4 py-2 text-sm font-medium text-accent hover:bg-accent/25"
    : "rounded-md border border-border/70 px-4 py-2 text-sm text-muted hover:border-accent/60 hover:text-text";
  if (href.startsWith("/openapi.json")) {
    const apiBase = process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000";
    return (
      <a href={`${apiBase}${href}`} target="_blank" rel="noreferrer" className={className}>
        {label}
      </a>
    );
  }
  return (
    <Link href={href} className={className}>
      {label}
    </Link>
  );
}

function Features() {
  return (
    <section className="grid gap-5 lg:grid-cols-3">
      {FEATURES.map((feature) => (
        <FeatureCard key={feature.title} feature={feature} />
      ))}
    </section>
  );
}

function FeatureCard({ feature }: { feature: Feature }) {
  return (
    <article className="flex flex-col gap-3 rounded-2xl border border-border/70 bg-surface/40 p-5 shadow-[0_1px_0_0_rgba(255,255,255,0.04)_inset] backdrop-blur">
      <header className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-text">{feature.title}</h3>
        <StatusPill status={feature.status} />
      </header>
      <p className="text-sm leading-relaxed text-muted">{feature.body}</p>
      {feature.endpoint ? (
        <code className="mt-auto rounded-md border border-border/60 bg-bg/40 px-2 py-1 font-mono text-[11px] text-muted">
          {feature.endpoint}
        </code>
      ) : null}
    </article>
  );
}

function StatusPill({ status }: { status: "ready" | "soon" }) {
  if (status === "ready") {
    return (
      <span className="rounded-full bg-success/10 px-2 py-0.5 font-mono text-[10px] uppercase tracking-wide text-success">
        Live
      </span>
    );
  }
  return (
    <span className="rounded-full bg-warning/10 px-2 py-0.5 font-mono text-[10px] uppercase tracking-wide text-warning">
      Soon
    </span>
  );
}

function BottomCta() {
  return (
    <section className="rounded-2xl border border-border/70 bg-surface/40 p-8 backdrop-blur">
      <h2 className="font-display text-xl font-semibold text-text">Try it against live data</h2>
      <p className="mt-2 max-w-2xl text-sm text-muted">
        The dev console runs every public endpoint against a local FastAPI
        instance backed by NEXRAD CONUS data. Spin it up with{" "}
        <code className="font-mono text-text">make start</code> — one command,
        one terminal.
      </p>
      <div className="mt-4 flex flex-wrap gap-3">
        <Link
          href="/console"
          className="rounded-md border border-accent bg-accent/15 px-4 py-2 text-sm font-medium text-accent hover:bg-accent/25"
        >
          Open the dev console →
        </Link>
        <a
          href="https://github.com/erichare/aeroza#quickstart"
          target="_blank"
          rel="noreferrer"
          className="rounded-md border border-border/70 px-4 py-2 text-sm text-muted hover:border-accent/60 hover:text-text"
        >
          Run it locally
        </a>
      </div>
    </section>
  );
}

function Footer() {
  return (
    <footer className="mt-auto border-t border-border/60 pt-6 text-center text-[11px] text-muted/60">
      Aeroza · {new Date().getFullYear()} · github.com/erichare/aeroza
    </footer>
  );
}
