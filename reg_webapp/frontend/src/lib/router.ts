/**
 * A tiny hand-rolled, path-based client router (no routing-library dep). URLs
 * mirror the API: `/catalog` (root), `/catalog/scb/lisa` (register),
 * `/catalog/scb/lisa/kon` (binding leaf), `/catalog/scb/lisa/variants` (the
 * register's variants sub-resource), `/catalog/class/<slug>`. Navigation is
 * `history.pushState`; back/forward is `popstate`; internal `<a>` clicks are
 * intercepted so they don't full-reload.
 *
 * Production SPA fallback (deep-linking to `/catalog/...` on a cold load) is a
 * DEPLOY concern: the backend is a pure JSON API (no StaticFiles mount), and the
 * SPA is served by Cloudflare. Cloudflare must rewrite unknown non-`/api` paths
 * to `index.html` (a `_redirects` / 404-rewrite rule) — a MAINTAINER task,
 * consistent with the existing "Cloudflare edge-cache gate is a maintainer task"
 * pattern (see reg_webapp/DESIGN.md → SPA routing + production fallback). The Vite dev server already does this
 * fallback by default (`appType: 'spa'`), so `bun run dev` deep-links work.
 */

/** The parsed current route. `home` is `/` (the landing page, #675); `root` is
 * `/catalog` (the data browser); `catalog-node` carries the FQID path after
 * `/catalog/`; `project` is the authoring surface (A5.3c); `not-found` is
 * anything else. */
export type Route =
  | { name: "home" }
  | { name: "root" }
  | { name: "catalog-node"; fqidPath: string }
  // `group` is the concept-group SUBJECT page (#617):
  // `/catalog/group/<provider>/<register>/<key>`. Distinct from `catalog-node`
  // (an FQID path) — a group is NOT FQID-addressable; it has its own fixed-shape
  // route. `member` is the optional `?member=` focus hint (read off the query,
  // like `?period`, NOT the path — so refining it doesn't remount the view).
  | { name: "group"; provider: string; register: string; key: string }
  // `class-group` is the classification-umbrella SUBJECT page (#756):
  // `/catalog/group/class/<key>`. The classification sibling of `group` — a
  // classification umbrella (e.g. the SUN umbrella, key `sun`) is catalog-global
  // (no provider/register), so it carries only the `key`.
  | { name: "class-group"; key: string }
  // `variants` is a register's variants page (Y-79):
  // `/catalog/<provider>/<register>/variants`. The variant axis is a register
  // SUB-RESOURCE, not an FQID segment — the same fixed 3-seg shape with a
  // literal `variants` tail the API declares above its catch-all.
  | { name: "variants"; provider: string; register: string }
  | { name: "project" }
  // `doc` is the minimal documentation viewer (#394); `identifier` is the doc
  // filename after `/doc/` (a single path segment, decoded here like catalog).
  | { name: "doc"; identifier: string }
  | { name: "not-found"; path: string };

/** `decodeURIComponent` that returns `null` on a malformed percent-sequence
 * instead of throwing a `URIError`. Load-bearing: `parseRoute` runs at module
 * import (the `Router` singleton's `$state` initializer), so an unguarded throw
 * on a cold deep-link to e.g. `/catalog/%` would escape module evaluation and
 * white-screen the whole SPA rather than render a not-found page. */
function safeDecode(segment: string): string | null {
  try {
    return decodeURIComponent(segment);
  } catch {
    return null;
  }
}

/** Parse a pathname into a route. Trailing slashes are tolerated; a malformed
 * percent-encoded segment routes to not-found (never throws). */
export function parseRoute(pathname: string): Route {
  const path = pathname.replace(/\/+$/, "") || "/";
  if (path === "/") {
    // The landing/home page (#675) — split from the data browser at `/catalog`
    // so `/` can carry a welcome + entry points instead of the provider list.
    return { name: "home" };
  }
  if (path === "/catalog") {
    return { name: "root" };
  }
  if (path === "/project") {
    return { name: "project" };
  }
  if (path.startsWith("/doc/")) {
    // Decode the single filename segment (the router stores the human identifier;
    // the api layer re-encodes when fetching). A malformed percent-sequence
    // (`null`) or an empty identifier routes to not-found rather than fetching a
    // mangled id.
    const identifier = safeDecode(path.slice("/doc/".length));
    if (identifier === null || identifier === "") {
      return { name: "not-found", path };
    }
    return { name: "doc", identifier };
  }
  if (path.startsWith("/catalog/")) {
    // Decode each segment (the router stores the human FQID; the api layer
    // re-encodes per segment when fetching). A malformed percent-sequence
    // (`null`) or an empty segment (a `//` in the path, an invalid FQID) routes
    // to not-found rather than fetching a mangled FQID.
    const segments = path.slice("/catalog/".length).split("/").map(safeDecode);
    if (segments.includes(null) || segments.includes("")) {
      return { name: "not-found", path };
    }
    const segs = segments as string[];
    // The classification-group SUBJECT route (#756):
    // `/catalog/group/class/<key...>`. Checked BEFORE the register-group route
    // below so the literal `class` segment routes here (mirrors the backend
    // declaration order), not as a register group with provider=`class`.
    // Classification umbrellas have no provider/register — only a `key`.
    if (segs[0] === "group" && segs[1] === "class" && segs.length >= 3) {
      return { name: "class-group", key: segs.slice(2).join("/") };
    }
    // The concept-group SUBJECT route (#617):
    // `/catalog/group/<provider>/<register>/<key...>` (`group` prefix). It
    // mirrors the backend's `{key:path}` route, distinct from an FQID path — a
    // group isn't FQID-addressable. Anything `group/<provider>/<register>` with
    // no key falls through to `catalog-node` (a bogus FQID that 404s server-side).
    if (segs.length >= 4 && segs[0] === "group") {
      return {
        name: "group",
        provider: segs[1],
        register: segs[2],
        key: segs.slice(3).join("/"),
      };
    }
    // The register's variants page (Y-79): the fixed 3-seg
    // `<provider>/<register>/variants` shape the API declares above its
    // catch-all. `variants` is reserved in the variable slot of the slug grammar
    // at build time, so no variable FQID can shadow it.
    if (segs.length === 3 && segs[2] === "variants") {
      return { name: "variants", provider: segs[0], register: segs[1] };
    }
    return { name: "catalog-node", fqidPath: segs.join("/") };
  }
  return { name: "not-found", path };
}

/** The current location as a vanilla store: React reads it with
 * `useSyncExternalStore` (`use_router.ts`), the unported Svelte views through the
 * `router.svelte.ts` façade. A module-level singleton: one router per SPA. */
class Router {
  /** The location this router is showing, in two halves. `route` moves only when
   * the PATHNAME does: `parseRoute` is pure in the pathname, so a query-only
   * navigation keeps the route OBJECT, not just an equal one. That identity
   * matters as much as the value: handing consumers a structurally equal
   * replacement invalidates every route-derived prop, which refetched the
   * query-independent catalog node and remounted the article around the leaf's
   * period card, dropping the researcher's keyboard focus to the document body
   * mid-Apply (Y-65; see reg_webapp/DESIGN.md → SPA routing). */
  #pathname = window.location.pathname;
  #route: Route = parseRoute(this.#pathname);
  /** The `?query` string (with leading `?`, or empty). DISTINCT from `route`,
   * which is keyed on the PATHNAME only: the resolution state lives in the query
   * (A5.3b's single source of truth — deep-linkable / shareable /
   * back-forward-correct), and it is what a same-path/new-`?period` navigation
   * moves. */
  #search = window.location.search;
  #listeners = new Set<() => void>();

  constructor() {
    window.addEventListener("popstate", () => this.syncLocation());
  }

  get route(): Route {
    return this.#route;
  }

  get search(): string {
    return this.#search;
  }

  /** Register a change listener; returns its removal. An arrow property, so it
   * can be handed to `useSyncExternalStore` or `createSubscriber` unbound. */
  subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  private emit(): void {
    for (const listener of this.#listeners) listener();
  }

  /** Re-sync both halves off `window.location` — the one place that reads the
   * browser's location, called from the popstate handler AND from `go` after its
   * push — and notify once if anything moved. An unchanged pathname keeps
   * the route object, so everything keyed on it survives an Apply on the catalog
   * leaf's period control untouched. */
  private syncLocation(): void {
    const { pathname, search } = window.location;
    if (pathname === this.#pathname && search === this.#search) {
      return;
    }
    if (pathname !== this.#pathname) {
      this.#pathname = pathname;
      this.#route = parseRoute(pathname);
    }
    this.#search = search;
    this.emit();
  }

  /** Navigate to `url` (path + optional `?query`/`#hash`) via pushState (no
   * reload). A no-op when already at the full `url` (path + query + hash) — so a
   * same-path/new-query navigation is correctly NOT a no-op. An arrow property,
   * so it can be handed to RAC's `RouterProvider` unbound. */
  navigate = (url: string): void => {
    const current =
      window.location.pathname + window.location.search + window.location.hash;
    if (url === current) {
      return;
    }
    window.history.pushState({}, "", url);
    this.syncLocation();
  };

  /** Read a query parameter off `search`. Returns the decoded value, or `null`
   * when absent OR present-but-empty (`?period=`) — an empty modifier is "no
   * value", and callers treat it as absent. */
  getQueryParam(name: string): string | null {
    return new URLSearchParams(this.#search).get(name) || null;
  }
}

export const router = new Router();

/**
 * `<a>`-click interception. Internal left-clicks (no modifier, same-origin, no
 * `target`) pushState-navigate; everything else (external links, new-tab
 * modifiers, `download`) falls through to the browser. Keyboard activation of an
 * `<a>` dispatches a native `click`, so it's intercepted too.
 *
 * App.tsx listens on the document, so a link anywhere — in a React part, a Svelte
 * island or a portalled dialog — routes. A RAC `Link` routes itself through the
 * `RouterProvider` and prevents the default, which the first check below honours.
 */
export function onNavClick(event: MouseEvent): void {
  // Respect modifier-clicks / non-primary buttons (open-in-new-tab etc.).
  if (
    event.defaultPrevented ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey
  ) {
    return;
  }
  // `event.target` is an `EventTarget` and can be a non-Element; only an Element
  // has `.closest` (calling it on a Text node / null would throw).
  const target = event.target;
  if (!(target instanceof Element)) {
    return;
  }
  // `closest("a")` also matches an SVG `<a>` (SVGAElement), which lacks the
  // HTMLHyperlinkElementUtils URL fields (`origin`/`pathname`/…) read below;
  // narrow to HTMLAnchorElement so those reads are sound (and skip SVG anchors).
  const anchor = target.closest("a");
  if (!(anchor instanceof HTMLAnchorElement)) {
    return;
  }
  const href = anchor.getAttribute("href");
  // Only intercept internal, same-tab, non-download links.
  if (
    href === null ||
    anchor.target ||
    anchor.hasAttribute("download") ||
    anchor.origin !== window.location.origin ||
    !href.startsWith("/")
  ) {
    return;
  }
  // Only intercept paths the SPA router OWNS (root + `/catalog/...`). A
  // same-origin link to a SERVER route (`/api/...`, `/openapi.json`)
  // or any other path must fall through to the browser — pushState-routing it
  // would render the SPA not-found instead of hitting the server.
  if (parseRoute(anchor.pathname).name === "not-found") {
    return;
  }
  event.preventDefault();
  // Preserve query + hash so deep-link refinements (A5.3b's `?period`/`?variant`)
  // survive the pushState navigation rather than being silently dropped.
  router.navigate(anchor.pathname + anchor.search + anchor.hash);
}
