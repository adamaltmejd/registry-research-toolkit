// Edge router for the global catalog's hosted MCP. Forwards `/mcp` to the
// zone origin (the Fly app running `reg-meta serve`); all other paths return 404.
//
// `fetch(request)` on the same zone is a subrequest to the DNS origin — it does
// NOT re-enter this worker, and it runs through Cloudflare's cache, so the
// origin's ETag/Cache-Control contract governs API
// caching exactly as a classic proxied origin (#220).

interface Env {
  DEPLOY_VERSION: string;
  // "true" where the deployment's catalog is served over hosted MCP: the global
  // catalog and a steward whose artifact is public (crates/DESIGN.md, "Hosted MCP").
  ROUTE_MCP?: string;
  // Secret (`wrangler secret put EDGE_TOKEN`), equal to the origin's
  // REG_META_EDGE_TOKEN: it proves a request came through this worker, so the
  // origin rate-limits `/mcp` by the request's CF-Connecting-IP. Without it the
  // worker refuses every forward: the origin would put every edge client in the
  // Fly proxy's one bucket.
  EDGE_TOKEN?: string;
}

const MCP_PATH = /^\/mcp$/;

// Cache-generation versioning (#318): the zone cache key is the full URL
// including the query string, so stamping the per-deploy DEPLOY_VERSION onto
// every origin-bound URL makes pre-deploy cache entries unreachable the moment
// a new worker version goes live — no purge credentials needed, and the 24h
// edge TTL still bounds origin traffic within a deploy generation. (cf.cacheKey
// would be the purpose-built mechanism, but it's Enterprise-only.) The origin
// tolerates the extra param: `reg-meta serve` drops it before validating the
// query and before deriving the ETag. POSTs get the param too (harmless —
// they're never cached); branching on method isn't worth the asymmetry.
const VERSION_PARAM = "__edge_v";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const mcp = env.ROUTE_MCP === "true" && MCP_PATH.test(url.pathname);
    if (mcp) {
      if (!env.EDGE_TOKEN) {
        return new Response("Edge misconfigured: EDGE_TOKEN is not set", {
          status: 503,
          headers: { "cache-control": "no-store" },
        });
      }
      url.searchParams.set(VERSION_PARAM, env.DEPLOY_VERSION);
      const forwarded = new Request(url, request);
      forwarded.headers.set("x-edge-token", env.EDGE_TOKEN);
      return fetch(forwarded);
    }
    return notFound();
  },
} satisfies ExportedHandler<Env>;

function notFound(): Response {
  return new Response("Not found", { status: 404, headers: { "cache-control": "no-store" } });
}
