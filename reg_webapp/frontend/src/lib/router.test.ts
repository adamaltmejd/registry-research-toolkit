import { afterEach, describe, expect, it } from "vitest";
import { onNavClick, parseRoute } from "./router.svelte";

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
});
