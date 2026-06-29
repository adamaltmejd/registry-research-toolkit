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
 * pattern (see reg_webapp/DESIGN.md → SPA routing + production fallback). The Vite dev server already does this
 * fallback by default (`appType: 'spa'`), so `bun run dev` deep-links work.
 */

/** The parsed current route. `home` is `/` (the landing page, #675); `root` is
 * `/catalog` (the data browser); `catalog-node` carries the FQID path after
 * `/catalog/`; `project` is the authoring surface (A5.3c); `search` is the
 * results page (the query lives in `?q=`, not the path, #379); `not-found` is
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
  | { name: "project" }
  | { name: "search" }
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
  if (path === "/search") {
    // The query lives in `?q=` (read via getQueryParam), NOT the path — `route`
    // stays keyed on the pathname only (like `?period`), so a refined `?q=` while
    // already on `/search` doesn't remount the results view (#379).
    return { name: "search" };
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
    return { name: "catalog-node", fqidPath: segs.join("/") };
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
   * in the query (see reg_webapp/DESIGN.md → Catalog router structure; A5.3b's
   * single source of truth — deep-linkable /
   * shareable / back-forward-correct). Components read `getQueryParam("period")`
   * etc. and re-fetch when it changes. Kept in sync with the URL in BOTH the
   * popstate handler AND inside `navigate` after `pushState`. */
  search = $state<string>(window.location.search);
  /** Where the search view's close control returns. Updated only when entering
   * `/search` from a non-search route, so query refinements on `/search` do not
   * overwrite it. A cold deep-link falls back to the catalog root. */
  searchReturnUrl = $state<string>("/catalog");

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
    this.go(url, false);
  }

  /** Like `navigate`, but REPLACES the current history entry
   * (`history.replaceState`) instead of pushing a new one — so refining a search
   * query in place (the omnibox's per-keystroke `?q=` updates, #379) doesn't spam
   * the back-stack. Same no-op-when-equal guard as `navigate`. */
  replace(url: string): void {
    this.go(url, true);
  }

  /** Shared body of `navigate`/`replace`: no-op when already at the full `url`
   * (path + query + hash), else push or replace the history entry and re-sync the
   * reactive route + query off the new location. */
  private go(url: string, replace: boolean): void {
    const current =
      window.location.pathname + window.location.search + window.location.hash;
    if (url === current) {
      return;
    }
    const next = new URL(url, window.location.origin);
    const nextRoute = parseRoute(next.pathname);
    if (nextRoute.name === "search" && this.route.name !== "search") {
      this.searchReturnUrl = current;
    }
    if (replace) {
      window.history.replaceState({}, "", url);
    } else {
      window.history.pushState({}, "", url);
    }
    this.route = parseRoute(window.location.pathname);
    this.search = window.location.search;
  }

  /** Read a query parameter off the reactive `search` (so reads inside an
   * `$effect`/`$derived` re-run when the query changes). Returns the decoded
   * value, or `null` when absent OR present-but-empty (`?period=`) — an empty
   * modifier is "no value", and callers treat it as absent. */
  getQueryParam(name: string): string | null {
    return new URLSearchParams(this.search).get(name) || null;
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
