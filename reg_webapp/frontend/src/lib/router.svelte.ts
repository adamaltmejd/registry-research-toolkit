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
    // re-encodes per segment when fetching).
    const segments = path.slice("/catalog/".length).split("/").map(safeDecode);
    if (segments.includes(null)) {
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

  constructor() {
    window.addEventListener("popstate", () => {
      this.route = parseRoute(window.location.pathname);
    });
  }

  /** Navigate to `url` (path + optional `?query`/`#hash`) via pushState (no
   * reload), updating the reactive route. The route is keyed on the PATHNAME;
   * any query/hash is preserved in the URL (A5.3b reads `?period`/`?variant`
   * from `window.location.search`) but doesn't change which node is shown. A
   * no-op when already at `url` (avoids a duplicate history entry). */
  navigate(url: string): void {
    const current =
      window.location.pathname + window.location.search + window.location.hash;
    if (url === current) {
      return;
    }
    window.history.pushState({}, "", url);
    this.route = parseRoute(window.location.pathname);
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
  const anchor = target.closest("a");
  if (!anchor) {
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
