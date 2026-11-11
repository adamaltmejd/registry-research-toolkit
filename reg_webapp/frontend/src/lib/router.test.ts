import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { catalogHref } from "./catalog";
import { onNavClick, parseRoute, type Route, router } from "./router";

describe("parseRoute", () => {
  it("decodes percent-encoded segments", () => {
    expect(parseRoute("/catalog/scb/lisa/k%C3%B6n")).toEqual({
      name: "catalog-node",
      fqidPath: "scb/lisa/kön",
    });
  });

  it("routes a malformed percent-sequence to not-found (never throws)", () => {
    // `decodeURIComponent("%")` throws a URIError; parseRoute runs at module
    // import (the Router singleton's field init), so an unguarded throw would
    // white-screen the SPA on a cold deep-link. Must degrade to not-found.
    expect(parseRoute("/catalog/%")).toEqual({
      name: "not-found",
      path: "/catalog/%",
    });
    expect(parseRoute("/catalog/scb/%E0%A4")).toEqual({
      name: "not-found",
      path: "/catalog/scb/%E0%A4",
    });
  });

  it("routes an empty path segment (a //) to not-found", () => {
    expect(parseRoute("/catalog/scb//lisa")).toEqual({
      name: "not-found",
      path: "/catalog/scb//lisa",
    });
  });
});

describe("catalog href builders ↔ parseRoute", () => {
  it("parses every built href back to the route it names", () => {
    // Non-ASCII and `%` must survive the encode → decode
    // round trip as the same route. An unencoded `%` would decode to not-found.
    const cases: [href: string, route: Route][] = [
      [
        catalogHref("scb/lisa/kön%"),
        { name: "catalog-node", fqidPath: "scb/lisa/kön%" },
      ],
    ];
    for (const [href, route] of cases) {
      expect(parseRoute(href), href).toEqual(route);
    }
  });
});

describe("onNavClick", () => {
  afterEach(() => {
    document.body.innerHTML = "";
  });

  // Mirror the production wiring: `onNavClick` is delegated from a container
  // (the `link` action). Build a container → anchor, attach the handler to the
  // container, and dispatch a bubbling click from the anchor.
  function clickAnchor(
    href: string,
    opts: { target?: string; download?: boolean; modifier?: boolean } = {},
  ): MouseEvent {
    const container = document.createElement("div");
    container.addEventListener("click", onNavClick);
    const a = document.createElement("a");
    a.setAttribute("href", href);
    if (opts.target) a.target = opts.target;
    if (opts.download) a.setAttribute("download", "");
    container.appendChild(a);
    document.body.appendChild(container);
    const event = new MouseEvent("click", {
      bubbles: true,
      cancelable: true,
      button: 0,
      metaKey: opts.modifier ?? false,
    });
    a.dispatchEvent(event);
    return event;
  }

  it("intercepts an internal same-origin link (preventDefault)", () => {
    const event = clickAnchor("/catalog/scb");
    expect(event.defaultPrevented).toBe(true);
  });

  it("ignores a modifier-click (open-in-new-tab)", () => {
    const event = clickAnchor("/catalog/scb", { modifier: true });
    expect(event.defaultPrevented).toBe(false);
  });

  it("ignores a target=_blank link", () => {
    const event = clickAnchor("/catalog/scb", { target: "_blank" });
    expect(event.defaultPrevented).toBe(false);
  });

  it("ignores a download link", () => {
    const event = clickAnchor("/api/project/order", { download: true });
    expect(event.defaultPrevented).toBe(false);
  });

  it("ignores an external (absolute) link", () => {
    const event = clickAnchor("https://scb.se/");
    expect(event.defaultPrevented).toBe(false);
  });

  it("ignores a same-origin link to a non-SPA path (server routes)", () => {
    // /api/* and /openapi.json are server routes — intercepting them would
    // pushState-route to the SPA not-found instead of hitting the server.
    expect(clickAnchor("/api/project/validate").defaultPrevented).toBe(false);
    expect(clickAnchor("/openapi.json").defaultPrevented).toBe(false);
  });
});

describe("router reactive query (A5.3b)", () => {
  // The `router` singleton reads `window.location`; reset to a known URL before
  // each case (jsdom's pushState updates `window.location`). Re-sync the signal
  // via `navigate` so the singleton's `search` matches the reset location.
  beforeEach(() => {
    // Reset to a KNOWN location DIFFERENT from the target first, so the navigate
    // below isn't a no-op (the guard compares the full URL) and actually re-syncs
    // the singleton's reactive route/search regardless of where a prior test left
    // it.
    window.history.pushState({}, "", "/");
    router.navigate("/catalog/scb/lisa/kon");
  });
  afterEach(() => {
    window.history.pushState({}, "", "/");
  });

  it("returns null for a present-but-empty query param (?period=)", () => {
    // An empty modifier is "no value" — A5.3b callers treat `?period=` as absent
    // (the `|| null` in getQueryParam). Pin it so a regression to `""` is caught.
    router.navigate("/catalog/scb/lisa/kon?period=");
    expect(router.getQueryParam("period")).toBeNull();
  });

  it("updates `search` on a same-path/new-query navigation (NOT a no-op)", () => {
    router.navigate("/catalog/scb/lisa/kon?period=2019");
    expect(router.getQueryParam("period")).toBe("2019");
    // Same pathname, different query — must update (the no-op guard compares the
    // FULL url, so this is correctly NOT a no-op).
    router.navigate("/catalog/scb/lisa/kon?period=2021");
    expect(router.getQueryParam("period")).toBe("2021");
  });

  it("updates `search` on a popstate event, without moving the route (Y-65)", () => {
    // Simulate a back/forward landing on a URL with a query: jsdom doesn't sync
    // location from a synthetic popstate, so push the target URL first, then fire
    // popstate (mirroring the browser's order: location changes, then the event).
    const before = router.route;
    window.history.pushState({}, "", "/catalog/scb/lisa/kon?period=2017");
    window.dispatchEvent(new PopStateEvent("popstate"));
    expect(router.getQueryParam("period")).toBe("2017");
    expect(router.route).toBe(before);
  });
});
