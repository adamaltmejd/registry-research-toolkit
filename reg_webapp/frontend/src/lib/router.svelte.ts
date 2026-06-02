/**
 * A tiny hand-rolled, path-based client router (no routing-library dep). URLs
 * mirror the API: `/catalog` (root), `/catalog/scb/lisa` (register),
 * `/catalog/scb/lisa/kon` (binding leaf), `/catalog/class/<slug>`. Navigation is
 * `history.pushState`; back/forward is `popstate`; internal `<a>` clicks are
 * intercepted so they don't full-reload.
 *
 * Production SPA fallback (deep-linking to `/catalog/...` on a cold load) is a
 * DEPLOY concern: the backend is a pure JSON API (no StaticFiles mount), and the
 * SPA is served by Cloudflare. Cloudflare must rewrite unknown non-`/api` paths
 * to `index.html` (a `_redirects` / 404-rewrite rule) — a MAINTAINER task,
 * consistent with the existing "Cloudflare edge-cache gate is a maintainer task"
 * pattern (see reg_webapp/DESIGN.md §9.4). The Vite dev server already does this
 * fallback by default (`appType: 'spa'`), so `bun run dev` deep-links work.
 */

/** The parsed current route. `root` is `/` and `/catalog`; `catalog-node`
 * carries the FQID path after `/catalog/`; `not-found` is anything else. */
export type Route =
  | { name: "root" }
  | { name: "catalog-node"; fqidPath: string }
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
  if (path === "/" || path === "/catalog") {
    return { name: "root" };
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
    return { name: "catalog-node", fqidPath: (segments as string[]).join("/") };
  }
  return { name: "not-found", path };
}

/** Reactive current route. Svelte 5 rune state — components read `router.route`
 * and re-render on navigation. A module-level singleton: one router per SPA. */
class Router {
  route = $state<Route>(parseRoute(window.location.pathname));
  /** The reactive `?query` string (with leading `?`, or empty). DISTINCT from
   * `route`, which is keyed on the PATHNAME only: a same-path/new-`?period`
   * navigation produces a structurally-equal `Route` (so `route` doesn't change
   * and `{#key route.fqidPath}` doesn't remount), but the resolution state lives
   * in the query (§9.5, A5.3b's single source of truth — deep-linkable /
   * shareable / back-forward-correct). Components read `getQueryParam("period")`
   * etc. and re-fetch when it changes. Kept in sync with the URL in BOTH the
   * popstate handler AND inside `navigate` after `pushState`. */
  search = $state<string>(window.location.search);

  constructor() {
    window.addEventListener("popstate", () => {
      this.route = parseRoute(window.location.pathname);
      this.search = window.location.search;
    });
  }

  /** Navigate to `url` (path + optional `?query`/`#hash`) via pushState (no
   * reload), updating the reactive route + query. The route is keyed on the
   * PATHNAME; the `?query` drives the resolution state (A5.3b reads
   * `?period`/`?variant`/`?value_set_version` via `getQueryParam`). A no-op when
   * already at the full `url` (path + query + hash) — so a same-path/new-query
   * navigation is correctly NOT a no-op. */
  navigate(url: string): void {
    const current =
      window.location.pathname + window.location.search + window.location.hash;
    if (url === current) {
      return;
    }
    window.history.pushState({}, "", url);
    this.route = parseRoute(window.location.pathname);
    this.search = window.location.search;
  }

  /** Read a query parameter off the reactive `search` (so reads inside an
   * `$effect`/`$derived` re-run when the query changes). Returns the decoded
   * value, or `null` when absent. */
  getQueryParam(name: string): string | null {
    return new URLSearchParams(this.search).get(name);
  }
}

export const router = new Router();

/**
 * `<a>`-click interception. Internal left-clicks (no modifier, same-origin, no
 * `target`) pushState-navigate; everything else (external links, new-tab
 * modifiers, `download`) falls through to the browser. Keyboard activation of an
 * `<a>` dispatches a native `click`, so it's intercepted too.
 *
 * Wired via the `link` action (below) rather than an `onclick` attribute so the
 * delegating container stays a plain `<div>` with no spurious interactive-role
 * a11y requirement — the real interactive elements are the `<a>`s it contains.
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
  // same-origin link to a BACKEND route (`/api/...`, `/docs`, `/openapi.json`)
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

/**
 * Svelte action: delegate `<a>`-click interception from a container element.
 * `use:link` on the shell's root `<div>` captures bubbled clicks from any
 * internal link and pushState-navigates them. Returns a `destroy` so the
 * listener is removed if the node unmounts.
 */
export function link(node: HTMLElement): { destroy: () => void } {
  node.addEventListener("click", onNavClick);
  return {
    destroy() {
      node.removeEventListener("click", onNavClick);
    },
  };
}
