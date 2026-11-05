// Edge router: origin paths pass through to the incoming hostname's zone
// origin (the matching Fly app running `reg-meta serve`), everything else is
// the SPA (static assets with single-page-application fallback). A deployment
// without an ASSETS binding (wrangler.jsonc, the global catalog's MCP-only host)
// forwards `/mcp` alone and answers everything else 404.
//
// `fetch(request)` on the same zone is a subrequest to the DNS origin — it does
// NOT re-enter this worker, and it runs through Cloudflare's cache, so the
// origin's ETag/Cache-Control contract governs API
// caching exactly as a classic proxied origin (#220).

interface Env {
  ASSETS?: Fetcher;
  DEPLOY_VERSION?: string;
  // Secret (`wrangler secret put EDGE_TOKEN`), equal to the origin's
  // REG_META_EDGE_TOKEN: it proves a request came through this worker, so the
  // origin rate-limits `/mcp` by the request's CF-Connecting-IP. Without it the
  // worker refuses every forward: the origin would put every edge client in the
  // Fly proxy's one bucket.
  EDGE_TOKEN?: string;
}

// LOCKSTEP: must match assets.run_worker_first in wrangler.swecov.jsonc (same set,
// glob syntax there): the paths `reg-meta serve` answers. `/mcp` is MCP_PATH: a POST
// (never a navigation) reaches the worker even without run_worker_first.
const ORIGIN_PATHS = [/^\/api(\/|$)/, /^\/openapi\.json$/];
const MCP_PATH = /^\/mcp$/;
// Hashed static assets (assets.run_worker_first has `/assets/*` too): a miss is a
// 404, not the SPA fallback, which frontend/public/_headers would stamp immutable
// for a year under the asset's URL (#1454). A hashed asset is never HTML, so an
// HTML answer from the assets binding is the fallback.
const ASSETS_PATH = /^\/assets\//;

// Cache-generation versioning (#318): the zone cache key is the full URL
// including the query string, so stamping the per-deploy DEPLOY_VERSION onto
// API/OpenAPI origin-bound URLs makes pre-deploy cache entries unreachable when
// a new worker version goes live — no purge credentials needed, and the 24h
// edge TTL still bounds origin traffic within a deploy generation. (cf.cacheKey
// would be the purpose-built mechanism, but it's Enterprise-only.) The origin
// tolerates the extra param: `reg-meta serve` drops it before validating the
// query and before deriving the ETag. POSTs get the param too (harmless —
// they're never cached); branching on method isn't worth the asymmetry.
const VERSION_PARAM = "__edge_v";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (env.ASSETS && !env.DEPLOY_VERSION) {
      return new Response("Edge misconfigured: DEPLOY_VERSION is not set", {
        status: 503,
        headers: { "cache-control": "no-store" },
      });
    }
    const url = new URL(request.url);
    const mcp = MCP_PATH.test(url.pathname);
    const api = env.ASSETS && ORIGIN_PATHS.some((re) => re.test(url.pathname));
    const toOrigin = mcp || api;
    if (toOrigin) {
      if (!env.EDGE_TOKEN) {
        return new Response("Edge misconfigured: EDGE_TOKEN is not set", {
          status: 503,
          headers: { "cache-control": "no-store" },
        });
      }
      if (api) url.searchParams.set(VERSION_PARAM, env.DEPLOY_VERSION!);
      const forwarded = api ? new Request(url, request) : new Request(request);
      forwarded.headers.set("x-edge-token", env.EDGE_TOKEN);
      return fetch(forwarded);
    }
    if (!env.ASSETS) return notFound();
    const response = await env.ASSETS.fetch(request);
    if (
      ASSETS_PATH.test(url.pathname) &&
      response.headers.get("content-type")?.startsWith("text/html")
    ) {
      return notFound();
    }
    return response;
  },
} satisfies ExportedHandler<Env>;

function notFound(): Response {
  return new Response("Not found", { status: 404, headers: { "cache-control": "no-store" } });
}
