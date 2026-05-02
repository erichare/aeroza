import type { NextConfig } from "next";

/**
 * Aeroza dev console — Next.js 15 App Router config.
 *
 * `NEXT_PUBLIC_AEROZA_API_URL` lets contributors point the console at a
 * non-default backend (e.g. a remote staging instance). When unset the
 * client falls back to `http://localhost:8000`, which matches the FastAPI
 * default and `make dev`.
 */
const nextConfig: NextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  // The `@aeroza/sdk` workspace ships as TypeScript source (no build step
  // yet). Next can transpile it on the fly when listed here.
  transpilePackages: ["@aeroza/sdk"],
  env: {
    NEXT_PUBLIC_AEROZA_API_URL:
      process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000",
  },
};

export default nextConfig;
