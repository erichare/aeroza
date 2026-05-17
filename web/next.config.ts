import type { NextConfig } from "next";

/**
 * Aeroza dev console — Next.js 15 App Router config.
 *
 * `NEXT_PUBLIC_AEROZA_API_URL` lets contributors point the console at a
 * non-default backend (e.g. a remote staging instance). When unset the
 * client falls back to `http://localhost:8000`, which matches the FastAPI
 * default and `make dev`.
 *
 * `NEXT_PUBLIC_AEROZA_TILES_URL` points at the static R2 origin
 * (production: `https://tiles.aeroza.app`). When unset the dashboard
 * falls back to the on-demand FastAPI tile route — slower but works
 * without an R2 bucket, so `make dev` is still viable.
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
    // Empty default — buildRadarTileUrlTemplate's runtime guard
    // routes through the FastAPI fallback when this is blank.
    NEXT_PUBLIC_AEROZA_TILES_URL:
      process.env.NEXT_PUBLIC_AEROZA_TILES_URL ?? "",
  },
  // /demo → /replay: the Storm Replay route was renamed to match its
  // label and product name. Permanent redirect so external links
  // (README, deploy guides, blog posts) keep resolving.
  async redirects() {
    return [
      {
        source: "/demo",
        destination: "/replay",
        permanent: true,
      },
    ];
  },
};

export default nextConfig;
