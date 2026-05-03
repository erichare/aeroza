"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { AerozaGlyph } from "./AerozaLogo";
import { PulseStrip } from "./PulseStrip";

const NAV_ITEMS: ReadonlyArray<{ href: string; label: string }> = [
  { href: "/", label: "Home" },
  { href: "/map", label: "Map" },
  { href: "/demo", label: "Replay" },
  { href: "/calibration", label: "Calibration" },
  { href: "/console", label: "Console" },
  { href: "/docs", label: "Docs" },
];

const GITHUB_URL = "https://github.com/erichare/aeroza";

// Routes that suppress the centre PulseStrip — typically because they
// already have their own richer status header. /map's page-level strip
// shows alerts, freshness, and severity counts in much more detail; a
// duplicate three-pill summary above it would be visual noise.
const PULSE_STRIP_SUPPRESS: ReadonlySet<string> = new Set(["/map", "/demo"]);

/**
 * Top-of-page nav, shared across the whole site (landing, console, docs).
 *
 * Three slots: brand (left), pulse strip (centre, route-conditional),
 * nav links (right). The pulse strip pills are live system signals
 * (alerts active, MRMS freshness, last-hour MAE) so every page-load
 * reaffirms "yes the system is running" without the user having to
 * navigate to /map or /calibration.
 */
export function SiteNav() {
  const pathname = usePathname();
  const showPulse = !PULSE_STRIP_SUPPRESS.has(pathname ?? "");
  return (
    <header className="sticky top-0 z-50 border-b border-border/60 bg-bg/80 backdrop-blur">
      <div className="mx-auto flex h-12 w-full max-w-[1400px] items-center justify-between gap-3 px-6">
        <Link
          href="/"
          className="group flex items-center gap-2 text-accent"
          aria-label="Aeroza · home"
        >
          {/* The brass hook-echo glyph. group-hover/focus tints back to
              text-text so it integrates with the parent's hover state.
              16px reads cleanly next to the nav's 11px wordmark. */}
          <AerozaGlyph
            size={16}
            title={null}
            className="transition-colors group-hover:text-text"
          />
          <span className="font-mono text-[11px] uppercase tracking-[0.2em] text-text">
            Aeroza
          </span>
        </Link>

        {showPulse ? <PulseStrip /> : <span className="flex-1" />}

        <nav className="flex items-center gap-1 text-xs">
          {NAV_ITEMS.map((item) => {
            const active =
              item.href === "/" ? pathname === "/" : pathname?.startsWith(item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                className={[
                  "rounded-md px-2 py-1 transition-colors",
                  active
                    ? "bg-accent/10 text-accent"
                    : "text-muted hover:bg-border/40 hover:text-text",
                ].join(" ")}
              >
                {item.label}
              </Link>
            );
          })}
          <a
            href={GITHUB_URL}
            target="_blank"
            rel="noreferrer"
            className="rounded-md px-2 py-1 text-muted transition-colors hover:bg-border/40 hover:text-text"
          >
            GitHub ↗
          </a>
        </nav>
      </div>
    </header>
  );
}
