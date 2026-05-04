"use client";

import Script from "next/script";
import { useLayoutEffect } from "react";

interface ScalarMountProps {
  bundleUrl: string;
  specUrl: string;
  configJson: string;
}

/**
 * Wraps the Scalar standalone bundle so its DOM side effects don't leak to
 * the rest of the app on navigation. The bundle:
 *
 *   1. Adds `dark-mode` to `<body>` and never removes it.
 *   2. Injects multiple large `<style>` tags into `<head>`, including one
 *      that sets `html { background: rgb(15,15,15) }`.
 *
 * Both persist after the route component unmounts, so navigating from the
 * explorer to any other page leaves the app rendered with Scalar's dark
 * palette over the Meridian chrome — text becomes near-illegible.
 *
 * Fix: snapshot existing `<style>` elements and `body` classes on mount,
 * then on unmount remove anything that wasn't there before. Cheap, robust,
 * and survives Scalar bundle upgrades because it's defined entirely in
 * terms of the diff.
 */
export function ScalarMount({
  bundleUrl,
  specUrl,
  configJson,
}: ScalarMountProps) {
  useLayoutEffect(() => {
    const beforeStyles = new Set(
      Array.from(document.head.querySelectorAll("style")),
    );
    const beforeBodyClasses = new Set(document.body.classList);
    return () => {
      document.head.querySelectorAll("style").forEach((style) => {
        if (!beforeStyles.has(style)) style.remove();
      });
      Array.from(document.body.classList).forEach((cls) => {
        if (!beforeBodyClasses.has(cls)) document.body.classList.remove(cls);
      });
    };
  }, []);

  return (
    <>
      {/* `data-url` is the canonical attribute the v1.25 bundle reads to
          know which spec to fetch. The JSON-body form
          (``type="application/json"``) was inconsistent in this version
          and silently dropped the fetch — leaving the widget rendered
          but empty. `data-configuration` carries every other knob so we
          keep the metaData / theme / hideDarkModeToggle hints, applied
          in addition to the URL above. */}
      <script
        id="api-reference"
        data-url={specUrl}
        data-configuration={configJson}
      />
      <Script src={bundleUrl} strategy="afterInteractive" />
    </>
  );
}
