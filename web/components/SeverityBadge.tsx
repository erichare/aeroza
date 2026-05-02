import type { Severity } from "@/lib/api";

// On the parchment theme the background tints are barely visible at low
// alpha, so each severity gets a slightly stronger fill than the dark-mode
// version while keeping text contrast (text + border use the saturated
// palette colours directly).
const SEVERITY_STYLES: Record<Severity, string> = {
  Extreme: "bg-danger/15 text-danger border-danger/50",
  Severe: "bg-warning/15 text-warning border-warning/50",
  Moderate: "bg-accent/15 text-accent border-accent/50",
  Minor: "bg-success/15 text-success border-success/50",
  Unknown: "bg-muted/15 text-muted border-muted/50",
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
