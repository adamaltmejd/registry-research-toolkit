// Edge router: origin paths pass through to the incoming hostname's zone
// origin (the matching Fly app running `reg-meta serve`), everything else is
// the SPA (static assets with single-page-application fallback).
//
// `fetch(request)` on the same zone is a subrequest to the DNS origin — it does
// NOT re-enter this worker, and it runs through Cloudflare's cache, so the
// origin's ETag/Cache-Control contract governs API
// caching exactly as a classic proxied origin (REFACTOR_SPEC.md §6.5 / #220).

interface Env {
  ASSETS: Fetcher;
  DEPLOY_VERSION: string;
  // "true" in wrangler.jsonc only: the global catalog is the hosted MCP endpoint
  // (RUST_RUNTIME_SPEC.md decision 12); a steward deployment serves none.
  ROUTE_MCP?: string;
  // Secret (`wrangler secret put EDGE_TOKEN`), equal to the origin's
  // REG_META_EDGE_TOKEN: it proves a request came through this worker, so the
  // origin rate-limits `/mcp` by the request's CF-Connecting-IP.
  EDGE_TOKEN?: string;
}

// LOCKSTEP: must match assets.run_worker_first in wrangler.jsonc (same set,
// glob syntax there): the paths `reg-meta serve` answers. `/mcp` joins only where
// ROUTE_MCP is set — a POST (never a navigation) reaches the worker even without
// run_worker_first, so the steward worker must refuse to forward it here.
const ORIGIN_PATHS = [/^\/api(\/|$)/, /^\/openapi\.json$/];
const MCP_PATH = /^\/mcp$/;

// Cache-generation versioning (#318): the zone cache key is the full URL
// including the query string, so stamping the per-deploy DEPLOY_VERSION onto
// every origin-bound URL makes pre-deploy cache entries unreachable the moment
// a new worker version goes live — no purge credentials needed, and the 24h
// edge TTL still bounds origin traffic within a deploy generation. (cf.cacheKey
// would be the purpose-built mechanism, but it's Enterprise-only.) The origin
// tolerates the extra param: `reg-meta serve` drops it before validating the
// query, and the ETag is content-derived. POSTs get the param too (harmless —
// they're never cached); branching on method isn't worth the asymmetry.
const VERSION_PARAM = "__edge_v";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const toOrigin =
      ORIGIN_PATHS.some((re) => re.test(url.pathname)) ||
      (env.ROUTE_MCP === "true" && MCP_PATH.test(url.pathname));
    if (toOrigin) {
      url.searchParams.set(VERSION_PARAM, env.DEPLOY_VERSION);
      const forwarded = new Request(url, request);
      if (env.EDGE_TOKEN) forwarded.headers.set("x-edge-token", env.EDGE_TOKEN);
      return fetch(forwarded);
    }
    return env.ASSETS.fetch(request);
  },
} satisfies ExportedHandler<Env>;
