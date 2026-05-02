"use client";

import { useEffect, useRef, useState } from "react";

import { alertsStreamUrl, type AlertProperties } from "./api";

export type ConnectionState = "connecting" | "open" | "error" | "closed";

/**
 * `EventSource` payloads from `/v1/alerts/stream`. The server emits raw
 * NWS-aliased JSON; we only need the user-facing properties subset for the
 * console feed (geometry can be added later if we wire in a map).
 */
export interface StreamedAlert extends AlertProperties {
  receivedAt: number;
}

export interface UseAlertStream {
  state: ConnectionState;
  events: StreamedAlert[];
  error: string | null;
  reconnect: () => void;
  clear: () => void;
}

const MAX_EVENTS = 100;

/**
 * Subscribe to `/v1/alerts/stream` and surface a rolling buffer of the
 * most recent alerts. Auto-reconnects via the browser's built-in EventSource
 * retry; we expose `reconnect()` for an explicit user-triggered retry.
 *
 * The hook intentionally keeps state minimal — the panel that renders the
 * feed can do its own derived computation (counts, severity rollup) on top.
 */
export function useAlertStream(): UseAlertStream {
  const [state, setState] = useState<ConnectionState>("connecting");
  const [events, setEvents] = useState<StreamedAlert[]>([]);
  const [error, setError] = useState<string | null>(null);
  const sourceRef = useRef<EventSource | null>(null);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    const url = alertsStreamUrl();
    let cancelled = false;
    const source = new EventSource(url);
    sourceRef.current = source;
    setState("connecting");
    setError(null);

    source.onopen = () => {
      if (cancelled) return;
      setState("open");
      setError(null);
    };

    source.addEventListener("alert", (rawEvent) => {
      if (cancelled) return;
      try {
        const payload = JSON.parse((rawEvent as MessageEvent).data) as AlertProperties;
        setEvents((prev) => {
          const next: StreamedAlert = { ...payload, receivedAt: Date.now() };
          const merged = [next, ...prev];
          return merged.slice(0, MAX_EVENTS);
        });
      } catch (err) {
        // Malformed payload — surface but don't tear down the connection.
        // EventSource will keep the underlying socket open.
        console.error("Failed to parse alert SSE payload", err);
      }
    });

    source.onerror = () => {
      if (cancelled) return;
      // EventSource auto-retries on transient errors; a closed readyState
      // means the browser has given up. Either way, surface the failure.
      if (source.readyState === EventSource.CLOSED) {
        setState("closed");
        setError("Connection closed by server.");
      } else {
        setState("error");
        setError("Stream error — retrying…");
      }
    };

    return () => {
      cancelled = true;
      source.close();
    };
  }, [nonce]);

  return {
    state,
    events,
    error,
    reconnect: () => setNonce((n) => n + 1),
    clear: () => setEvents([]),
  };
}
