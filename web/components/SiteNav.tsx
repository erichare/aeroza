"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV_ITEMS: ReadonlyArray<{ href: string; label: string }> = [
  { href: "/", label: "Home" },
  { href: "/map", label: "Map" },
  { href: "/console", label: "Console" },
  { href: "/docs", label: "Docs" },
];

const GITHUB_URL = "https://github.com/erichare/aeroza";

/**
 * Top-of-page nav, shared across the whole site (landing, console, docs).
 *
 * Kept deliberately small — three internal links + a GitHub external — and
 * route-aware via `usePathname` so the active route is highlighted without
 * us tracking it ourselves.
 */
export function SiteNav() {
  const pathname = usePathname();
  return (
    <header className="sticky top-0 z-50 border-b border-border/60 bg-bg/80 backdrop-blur">
      <div className="mx-auto flex h-12 w-full max-w-[1400px] items-center justify-between gap-3 px-6">
        <Link href="/" className="flex items-center gap-2">
          <span className="inline-block h-2 w-2 rounded-full bg-accent pulse-dot" />
          <span className="font-mono text-[11px] uppercase tracking-[0.2em] text-text">
            Aeroza
          </span>
        </Link>

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
