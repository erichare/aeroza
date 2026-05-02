/**
 * Tiny formatting helpers shared by panels. Kept dependency-free so the
 * console doesn't pull in date-fns / numeral just to render a few labels.
 */

const UNITS = ["B", "KB", "MB", "GB", "TB"] as const;

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const i = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    UNITS.length - 1,
  );
  return `${(bytes / 1024 ** i).toFixed(i === 0 ? 0 : 1)} ${UNITS[i]}`;
}

const MINUTE = 60;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/**
 * "3m ago", "2h 15m ago", "in 5d 21h" — cascades two units when the gap is
 * large enough to warrant it. Past and future are both formatted the same way
 * so a single helper covers `latestExpires` (future) and `latestValidAt` (past).
 */
export function formatRelative(date: Date, now: number = Date.now()): string {
  const deltaSeconds = Math.floor((now - date.getTime()) / 1000);
  const past = deltaSeconds >= 0;
  const seconds = Math.abs(deltaSeconds);
  const body = humanizeDuration(seconds);
  return past ? `${body} ago` : `in ${body}`;
}

function humanizeDuration(seconds: number): string {
  if (seconds < MINUTE) return `${seconds}s`;
  if (seconds < HOUR) {
    const m = Math.floor(seconds / MINUTE);
    return `${m}m`;
  }
  if (seconds < DAY) {
    const h = Math.floor(seconds / HOUR);
    const m = Math.floor((seconds % HOUR) / MINUTE);
    return m > 0 ? `${h}h ${m}m` : `${h}h`;
  }
  const d = Math.floor(seconds / DAY);
  const h = Math.floor((seconds % DAY) / HOUR);
  return h > 0 ? `${d}d ${h}h` : `${d}d`;
}
