type Tone = "success" | "warning" | "danger" | "muted";

const TONE_CLASSES: Record<Tone, string> = {
  success: "bg-success",
  warning: "bg-warning",
  danger: "bg-danger",
  muted: "bg-muted",
};

interface StatusDotProps {
  tone: Tone;
  label: string;
  pulse?: boolean;
}

/**
 * Coloured dot + caption — used for SSE connection status, severity badges,
 * health indicators. Pulse animates the dot for "live" states.
 */
export function StatusDot({ tone, label, pulse = false }: StatusDotProps) {
  return (
    <span className="inline-flex items-center gap-2 text-xs">
      <span
        className={[
          "h-2 w-2 rounded-full",
          TONE_CLASSES[tone],
          pulse ? "pulse-dot" : "",
        ].join(" ")}
      />
      <span className="font-medium text-muted">{label}</span>
    </span>
  );
}
