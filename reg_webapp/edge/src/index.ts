// Edge router: backend paths pass through to the zone origin (Fly), everything
// else is the SPA (static assets with single-page-application fallback).
//
// `fetch(request)` on the same zone is a subrequest to the DNS origin — it does
// NOT re-enter this worker, and it runs through Cloudflare's cache, so the
// origin's ETag/Cache-Control contract (reg_webapp middleware) governs API
// caching exactly as a classic proxied origin (REFACTOR_SPEC.md §6.5 / #220).

interface Env {
  ASSETS: Fetcher;
}

// LOCKSTEP: must match assets.run_worker_first in wrangler.jsonc (same set,
// glob syntax there). The backend serves exactly these top-level paths
// (create_app disables /redoc for this reason).
const ORIGIN_PATHS = [/^\/api(\/|$)/, /^\/openapi\.json$/, /^\/docs(\/|$)/];

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);
    if (ORIGIN_PATHS.some((re) => re.test(pathname))) {
      return fetch(request);
    }
    return env.ASSETS.fetch(request);
  },
} satisfies ExportedHandler<Env>;
