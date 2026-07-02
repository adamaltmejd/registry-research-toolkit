// Edge router: backend paths pass through to the incoming hostname's zone
// origin (the matching Fly app), everything else is the SPA (static assets
// with single-page-application fallback).
//
// `fetch(request)` on the same zone is a subrequest to the DNS origin — it does
// NOT re-enter this worker, and it runs through Cloudflare's cache, so the
// origin's ETag/Cache-Control contract (reg_webapp middleware) governs API
// caching exactly as a classic proxied origin (REFACTOR_SPEC.md §6.5 / #220).

interface Env {
  ASSETS: Fetcher;
  DEPLOY_VERSION: string;
}

// LOCKSTEP: must match assets.run_worker_first in wrangler.jsonc (same set,
// glob syntax there). The backend serves exactly these top-level paths
// (create_app disables /redoc for this reason).
const ORIGIN_PATHS = [/^\/api(\/|$)/, /^\/openapi\.json$/, /^\/docs(\/|$)/];

// Cache-generation versioning (#318): the zone cache key is the full URL
// including the query string, so stamping the per-deploy DEPLOY_VERSION onto
// every origin-bound URL makes pre-deploy cache entries unreachable the moment
// a new worker version goes live — no purge credentials needed, and the 24h
// edge TTL still bounds origin traffic within a deploy generation. (cf.cacheKey
// would be the purpose-built mechanism, but it's Enterprise-only.) The origin
// tolerates the extra param: FastAPI ignores undeclared query params, and the
// ETag is content-derived. POSTs get the param too (harmless — they're never
// cached); branching on method isn't worth the asymmetry.
const VERSION_PARAM = "__edge_v";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (ORIGIN_PATHS.some((re) => re.test(url.pathname))) {
      url.searchParams.set(VERSION_PARAM, env.DEPLOY_VERSION);
      return fetch(new Request(url, request));
    }
    return env.ASSETS.fetch(request);
  },
} satisfies ExportedHandler<Env>;
