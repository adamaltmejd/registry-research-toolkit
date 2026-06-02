import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { onNavClick, parseRoute, router } from "./router.svelte";

describe("parseRoute", () => {
  it("maps / and /catalog to the root", () => {
    expect(parseRoute("/")).toEqual({ name: "root" });
    expect(parseRoute("/catalog")).toEqual({ name: "root" });
    expect(parseRoute("/catalog/")).toEqual({ name: "root" }); // trailing slash
  });

  it("parses a catalog node path into its FQID path", () => {
    expect(parseRoute("/catalog/scb")).toEqual({
      name: "catalog-node",
      fqidPath: "scb",
    });
    expect(parseRoute("/catalog/scb/lisa/kon")).toEqual({
      name: "catalog-node",
      fqidPath: "scb/lisa/kon",
    });
  });

  it("decodes percent-encoded segments", () => {
    expect(parseRoute("/catalog/scb/lisa/k%C3%B6n")).toEqual({
      name: "catalog-node",
      fqidPath: "scb/lisa/kön",
    });
  });

  it("routes a malformed percent-sequence to not-found (never throws)", () => {
    // `decodeURIComponent("%")` throws a URIError; parseRoute runs at module
    // import (the Router singleton's $state init), so an unguarded throw would
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

  it("treats the classification axis as a catalog node", () => {
    expect(parseRoute("/catalog/class/sun2020")).toEqual({
      name: "catalog-node",
      fqidPath: "class/sun2020",
    });
  });

  it("maps anything else to not-found", () => {
    expect(parseRoute("/about")).toEqual({ name: "not-found", path: "/about" });
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

  it("ignores a same-origin link to a non-SPA path (backend routes, /docs)", () => {
    // /api/* and the FastAPI docs are server routes — intercepting them would
    // pushState-route to the SPA not-found instead of hitting the backend.
    expect(clickAnchor("/api/project/validate").defaultPrevented).toBe(false);
    expect(clickAnchor("/openapi.json").defaultPrevented).toBe(false);
    expect(clickAnchor("/docs").defaultPrevented).toBe(false);
  });

  it("intercepts an SPA route under /catalog", () => {
    expect(clickAnchor("/catalog/scb/lisa/kon").defaultPrevented).toBe(true);
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

  it("reads ?period off the query after a navigate", () => {
    router.navigate("/catalog/scb/lisa/kon?period=2020");
    expect(router.getQueryParam("period")).toBe("2020");
  });

  it("returns null for an absent query param", () => {
    router.navigate("/catalog/scb/lisa/kon?period=2020");
    expect(router.getQueryParam("variant")).toBeNull();
  });

  it("reads multiple modifiers off the query", () => {
    router.navigate(
      "/catalog/scb/lisa/kon?period=2020&variant=x&value_set_version=y",
    );
    expect(router.getQueryParam("period")).toBe("2020");
    expect(router.getQueryParam("variant")).toBe("x");
    expect(router.getQueryParam("value_set_version")).toBe("y");
  });

  it("updates `search` on a same-path/new-query navigation (NOT a no-op)", () => {
    router.navigate("/catalog/scb/lisa/kon?period=2019");
    expect(router.getQueryParam("period")).toBe("2019");
    // Same pathname, different query — must update (the no-op guard compares the
    // FULL url, so this is correctly NOT a no-op).
    router.navigate("/catalog/scb/lisa/kon?period=2021");
    expect(router.getQueryParam("period")).toBe("2021");
  });

  it("clears the query when navigating to the bare pathname", () => {
    router.navigate("/catalog/scb/lisa/kon?period=2020");
    expect(router.getQueryParam("period")).toBe("2020");
    router.navigate("/catalog/scb/lisa/kon");
    expect(router.getQueryParam("period")).toBeNull();
  });

  it("updates `search` on a popstate event", () => {
    // Simulate a back/forward landing on a URL with a query: jsdom doesn't sync
    // location from a synthetic popstate, so push the target URL first, then fire
    // popstate (mirroring the browser's order: location changes, then the event).
    window.history.pushState({}, "", "/catalog/scb/lisa/kon?period=2017");
    window.dispatchEvent(new PopStateEvent("popstate"));
    expect(router.getQueryParam("period")).toBe("2017");
  });

  // NOTE: the *reactivity* of `search` (a $derived/$effect reading getQueryParam
  // re-running when navigate/popstate mutates it) is not unit-tested here — the
  // router is a module-level singleton whose $state lives outside any reactive
  // root, so an isolated `$effect.root` in a test doesn't connect to it. The seam
  // is covered instead by: async.svelte.test.ts (asyncResource's $effect re-runs
  // on a tracked $state change — the same mechanism) + the imperative cases above
  // (the value updates) + the A5.3b visual gate (the period resolve refetched
  // without a pathname remount).
});
