import type { ReactNode } from "react";

interface PanelProps {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
}

/**
 * Glass-card panel — the primary layout unit of the console.
 * Each major view (alerts stream, MRMS files, health) lives inside one.
 */
export function Panel({ title, subtitle, actions, children, className }: PanelProps) {
  return (
    <section
      className={[
        "flex flex-col rounded-2xl border border-border/70 bg-surface/40 backdrop-blur",
        "shadow-[0_1px_0_0_rgba(255,255,255,0.04)_inset]",
        className ?? "",
      ].join(" ")}
    >
      <header className="flex items-start justify-between gap-3 border-b border-border/60 px-5 py-4">
        <div>
          <h2 className="text-sm font-semibold tracking-tight text-text">{title}</h2>
          {subtitle ? (
            <p className="mt-0.5 text-xs text-muted">{subtitle}</p>
          ) : null}
        </div>
        {actions ? <div className="flex items-center gap-2">{actions}</div> : null}
      </header>
      <div className="flex-1 overflow-hidden">{children}</div>
    </section>
  );
}
