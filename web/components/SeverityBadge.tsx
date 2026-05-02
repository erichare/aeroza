import type { Severity } from "@/lib/api";

const SEVERITY_STYLES: Record<Severity, string> = {
  Extreme: "bg-danger/20 text-danger border-danger/40",
  Severe: "bg-warning/20 text-warning border-warning/40",
  Moderate: "bg-accent/15 text-accent border-accent/40",
  Minor: "bg-success/15 text-success border-success/40",
  Unknown: "bg-muted/15 text-muted border-muted/40",
};

export function SeverityBadge({ severity }: { severity: Severity }) {
  return (
    <span
      className={[
        "inline-flex items-center rounded-md border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide",
        SEVERITY_STYLES[severity] ?? SEVERITY_STYLES.Unknown,
      ].join(" ")}
    >
      {severity}
    </span>
  );
}
