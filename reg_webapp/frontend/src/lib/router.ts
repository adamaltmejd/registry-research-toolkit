/**
 * A tiny hand-rolled, path-based client router (no routing-library dep). URLs
 * mirror the API: `/catalog/scb/lisa` (register),
 * `/catalog/scb/lisa/kon` (variable), `/catalog/class/<slug>`. Navigation is
 * `history.pushState`; back/forward is `popstate`; internal `<a>` clicks are
 * intercepted so they don't full-reload.
 *
 * A cold load of a deep link (`/catalog/...`) is the edge's job: a steward's edge
 * worker serves `index.html` for any path that is not an asset or the API
 * (`single-page-application` fallback, reg_webapp/DESIGN.md → Steward repositories). The Vite
 * dev server does the same (`appType: 'spa'`), so `bun run dev` deep-links work.
 */

/** The parsed current route. `home` is `/` (the landing page, #675);
 * `catalog-node` carries the FQID path after `/catalog/`; `project` is the
 * authoring surface (A5.3c); `not-found` is anything else. `/catalog` itself
 * is no page: it is `home`, and the router rewrites the address to `/`
 * (maintainer 2026-11-11, #2403). */
export type Route =
  | { name: "home" }
  | { name: "catalog-node"; fqidPath: string }
  | { name: "project" }
  | { name: "not-found"; path: string };

/** `decodeURIComponent` that returns `null` on a malformed percent-sequence
 * instead of throwing a `URIError`. Load-bearing: `parseRoute` runs at module
 * import (the `Router` singleton's field initializer), so an unguarded throw
 * on a cold deep-link to e.g. `/catalog/%` would escape module evaluation and
 * white-screen the whole SPA rather than render a not-found page. */
function safeDecode(segment: string): string | null {
  try {
    return decodeURIComponent(segment);
  } catch {
    return null;
  }
}

/** The data browser's old root page, retired (maintainer 2026-11-11, #2403):
 * the browse columns stand beside every page, so it showed nothing of its own. */
const CATALOG_ROOT = "/catalog";

/** `/catalog`, with or without trailing slashes, becomes `/` in place: no
 * reload and no new history entry, its query and hash dropped. A cold load
 * reaches it through the edge's SPA fallback, a link through `navigate`. */
function redirectCatalogRoot(): void {
  if (window.location.pathname.replace(/\/+$/, "") === CATALOG_ROOT) {
    window.history.replaceState(window.history.state, "", "/");
  }
}

/** Parse a pathname into a route. Trailing slashes are tolerated; a malformed
 * percent-encoded segment routes to not-found (never throws). */
export function parseRoute(pathname: string): Route {
  const path = pathname.replace(/\/+$/, "") || "/";
  if (path === "/" || path === CATALOG_ROOT) {
    return { name: "home" };
  }
  if (path === "/project") {
    return { name: "project" };
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
    return { name: "catalog-node", fqidPath: segs.join("/") };
  }
  return { name: "not-found", path };
}

/** The current location as a vanilla store: React reads it with
 * `useSyncExternalStore` (`use_router.ts`). A module-level singleton: one router per SPA. */
class Router {
  /** The location this router is showing, in two halves. `route` moves only when
   * the PATHNAME does: `parseRoute` is pure in the pathname, so a query-only
   * navigation keeps the route OBJECT, not just an equal one. That identity
   * matters as much as the value: handing consumers a structurally equal
   * replacement invalidates every route-derived prop, which refetched the
   * query-independent catalog node and remounted the article around the leaf's
   * period card, dropping the researcher's keyboard focus to the document body
   * mid-Apply (Y-65). */
  #pathname = (redirectCatalogRoot(), window.location.pathname);
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
    redirectCatalogRoot();
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
   * so it can be handed to RAC's `RouterProvider` unbound. `replace` swaps the
   * current entry instead, for a control that commits on every step (a slider
   * key press) and must not leave one Back entry per step. */
  navigate = (url: string, { replace = false } = {}): void => {
    const current =
      window.location.pathname + window.location.search + window.location.hash;
    if (url === current) {
      return;
    }
    if (replace) window.history.replaceState({}, "", url);
    else window.history.pushState({}, "", url);
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
 * App.tsx listens on the document, so a link anywhere, including one in a portalled
 * dialog, routes. A RAC `Link` routes itself through the
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
