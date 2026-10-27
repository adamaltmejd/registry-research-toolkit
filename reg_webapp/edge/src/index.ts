// Edge router for the global catalog's hosted MCP. Forwards `/mcp` to the
// zone origin (the Fly app running `reg-meta serve`); all other paths return 404.
//
// A same-zone subrequest goes to the DNS origin without re-entering this worker.

interface Env {
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
      const forwarded = new Request(request);
      forwarded.headers.set("x-edge-token", env.EDGE_TOKEN);
      return fetch(forwarded);
    }
    return notFound();
  },
} satisfies ExportedHandler<Env>;

function notFound(): Response {
  return new Response("Not found", { status: 404, headers: { "cache-control": "no-store" } });
}
