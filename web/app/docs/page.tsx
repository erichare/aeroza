import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Docs",
  description:
    "Aeroza developer documentation. Quickstart, concepts, and full route reference.",
};

const API_BASE = process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000";

/**
 * Placeholder docs hub. The real content lands in the next PR (quickstart,
 * concepts, route reference). For now this page links out to the running
 * FastAPI's auto-generated Swagger UI so visitors aren't dead-ended.
 */
export default function DocsPlaceholderPage() {
  return (
    <main className="mx-auto flex min-h-[calc(100vh-3rem)] w-full max-w-[800px] flex-col gap-8 px-6 py-16">
      <header className="flex flex-col gap-3">
        <span className="rounded-full border border-warning/40 bg-warning/10 px-3 py-1 self-start font-mono text-[10px] uppercase tracking-[0.2em] text-warning">
          Docs hub · in progress
        </span>
        <h1 className="text-3xl font-semibold tracking-tight text-text">Aeroza docs</h1>
        <p className="max-w-2xl text-sm leading-relaxed text-muted">
          Real documentation lands in the next change — quickstart, concepts,
          and a per-route reference. Until then, the running FastAPI exposes
          the full surface as auto-generated Swagger UI.
        </p>
      </header>

      <section className="flex flex-col gap-3">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-muted">
          What you can use today
        </h2>
        <ul className="flex flex-col gap-3 text-sm">
          <DocLink
            href={`${API_BASE}/docs`}
            label="Swagger UI"
            external
            description="Interactive request builder for every public route."
          />
          <DocLink
            href={`${API_BASE}/openapi.json`}
            label="OpenAPI schema"
            external
            description="Raw JSON spec — point your codegen tool at it."
          />
          <DocLink
            href="/console"
            label="Dev console"
            description="Browser dashboard exercising the live API end-to-end."
          />
          <DocLink
            href="https://github.com/erichare/aeroza#readme"
            label="README"
            external
            description="Project overview, architecture, local quickstart."
          />
        </ul>
      </section>
    </main>
  );
}

interface DocLinkProps {
  href: string;
  label: string;
  description: string;
  external?: boolean;
}

function DocLink({ href, label, description, external = false }: DocLinkProps) {
  const className =
    "group flex flex-col gap-1 rounded-xl border border-border/70 bg-surface/40 px-4 py-3 transition-colors hover:border-accent/60";
  const labelEl = (
    <span className="text-sm font-medium text-text group-hover:text-accent">
      {label}
      {external ? <span className="text-muted"> ↗</span> : null}
    </span>
  );
  const descEl = <span className="text-xs text-muted">{description}</span>;

  if (external) {
    return (
      <li>
        <a href={href} target="_blank" rel="noreferrer" className={className}>
          {labelEl}
          {descEl}
        </a>
      </li>
    );
  }
  return (
    <li>
      <Link href={href} className={className}>
        {labelEl}
        {descEl}
      </Link>
    </li>
  );
}
