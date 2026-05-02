"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

interface DocsSection {
  href: string;
  label: string;
  description: string;
}

const SECTIONS: ReadonlyArray<DocsSection> = [
  { href: "/docs", label: "Overview", description: "What Aeroza is + where to go" },
  { href: "/docs/quickstart", label: "Quickstart", description: "Run it locally in 5 minutes" },
  { href: "/docs/concepts", label: "Concepts", description: "Alerts, MRMS, sample, polygon" },
  { href: "/docs/api", label: "API reference", description: "Routes + interactive Swagger" },
];

interface DocsLayoutProps {
  children: ReactNode;
}

/**
 * Two-column layout used by every /docs/* route: a left sidebar (with the
 * active section highlighted) and a content column. Lives in components/
 * rather than as a Next App Router `layout.tsx` so the placeholder route
 * `/docs` and the deeper routes can share it without forcing a layout
 * boundary that changes hydration cost.
 */
export function DocsLayout({ children }: DocsLayoutProps) {
  const pathname = usePathname();
  return (
    <main className="mx-auto flex w-full max-w-[1400px] flex-col gap-8 px-6 py-12 lg:flex-row lg:gap-12">
      <aside className="lg:sticky lg:top-16 lg:h-[calc(100vh-5rem)] lg:w-56 lg:shrink-0">
        <nav className="flex flex-col gap-1">
          <span className="mb-2 text-[10px] font-medium uppercase tracking-[0.2em] text-muted">
            Docs
          </span>
          {SECTIONS.map((section) => {
            const active =
              section.href === "/docs"
                ? pathname === "/docs"
                : pathname?.startsWith(section.href);
            return (
              <Link
                key={section.href}
                href={section.href}
                className={[
                  "group flex flex-col gap-0.5 rounded-md border px-3 py-2 transition-colors",
                  active
                    ? "border-accent/40 bg-accent/10"
                    : "border-transparent hover:border-border/70 hover:bg-surface/40",
                ].join(" ")}
              >
                <span
                  className={[
                    "text-sm font-medium",
                    active ? "text-accent" : "text-text",
                  ].join(" ")}
                >
                  {section.label}
                </span>
                <span className="text-[11px] text-muted">{section.description}</span>
              </Link>
            );
          })}
        </nav>
      </aside>
      <article className="prose-aeroza min-w-0 flex-1">{children}</article>
    </main>
  );
}
