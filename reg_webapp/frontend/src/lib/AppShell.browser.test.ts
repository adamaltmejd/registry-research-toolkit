import { createRawSnippet } from "svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import AppShell from "./AppShell.svelte";
import type { RootResponse } from "./api";
import { getCatalogRoot } from "./api";
import { DATA_BROWSER_LABEL } from "./catalog";
import { router } from "./router.svelte";

// Stub ONLY the catalog-root GET (the shell's provider-facet fetch); keep the
// rest of api.ts real — mirrors CatalogPicker / SearchOmnibox's partial-mock
// pattern (override the GET, leave types + helpers intact).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getCatalogRoot: vi.fn() };
});

/** A catalog root whose children are providers — the contextual facet list the
 * shell renders in its `aria-label="Providers"` nav. */
function rootResponse(...providers: string[]): RootResponse {
  return {
    kind: "root",
    children: providers.map((fqid) => ({
      kind: "provider",
      fqid,
      name: fqid,
    })),
  } as unknown as RootResponse;
}

/** The routed content the shell wraps — App passes its `<main>` switch in as the
 * `children` snippet. */
function body(text: string) {
  return createRawSnippet(() => ({ render: () => `<p>${text}</p>` }));
}

/** Minimal real-shaped props for the shell. The catalog-root fetch is mocked, so
 * the steward/window wiring is inert chrome for the nav-focused tests below. */
function minimalProps() {
  return {
    steward: { long_name: "Statistics Sweden", id: "scb" },
    windowMin: 1960,
    windowMax: 2026,
    windowValue: null,
    onWindowChange: () => {},
    onWindowClear: () => {},
    breadcrumbs: [{ label: DATA_BROWSER_LABEL }],
    children: body("routed content"),
  };
}

// The shell routes through the real `router` singleton (only getCatalogRoot is
// mocked). Each case resets the URL + re-syncs the singleton before rendering;
// the afterEach restores it so route state doesn't leak (mirrors SearchOmnibox).
function setUrl(path: string): void {
  window.history.pushState({}, "", "/__reset__");
  router.navigate(path);
}

beforeEach(() => {
  setUrl("/");
  vi.mocked(getCatalogRoot).mockReset();
  vi.mocked(getCatalogRoot).mockResolvedValue(rootResponse("scb", "sos"));
});

afterEach(() => {
  window.history.pushState({}, "", "/");
});

describe("AppShell — provider facets", () => {
  it("renders each provider as a link inside the Providers nav", async () => {
    await render(AppShell, minimalProps());

    const facets = page.getByRole("navigation", { name: "Providers" });
    await expect
      .element(facets.getByRole("link", { name: "scb" }))
      .toBeVisible();
    await expect
      .element(facets.getByRole("link", { name: "sos" }))
      .toBeVisible();
  });
});

describe("AppShell — active nav (aria-current)", () => {
  it("marks the data-browser link current on a catalog route", async () => {
    setUrl("/catalog");
    await render(AppShell, minimalProps());

    await expect
      .element(page.getByRole("link", { name: DATA_BROWSER_LABEL }))
      .toHaveAttribute("aria-current", "page");
    // The Project link is NOT current.
    expect(
      page.getByRole("link", { name: "Project" }).query(),
    ).not.toHaveAttribute("aria-current");
  });

  it("marks the Project link current on the project route (inverse)", async () => {
    setUrl("/project");
    await render(AppShell, minimalProps());

    await expect
      .element(page.getByRole("link", { name: "Project" }))
      .toHaveAttribute("aria-current", "page");
    expect(
      page.getByRole("link", { name: DATA_BROWSER_LABEL }).query(),
    ).not.toHaveAttribute("aria-current");
  });
});

describe("AppShell — mobile drawer", () => {
  it("toggles open via the hamburger and closes via the scrim", async () => {
    await render(AppShell, minimalProps());

    const toggle = page.getByRole("button", { name: "Open menu" });
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
    // The scrim only exists while the drawer is open.
    expect(page.getByRole("button", { name: "Close menu" }).query()).toBeNull();

    await toggle.click();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "true");
    const scrim = page.getByRole("button", { name: "Close menu" });
    await expect.element(scrim).toBeVisible();

    await scrim.click();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
    await expect
      .element(page.getByRole("button", { name: "Close menu" }))
      .not.toBeInTheDocument();
  });

  it("closes the drawer when the route changes (the close-on-navigate $effect)", async () => {
    await render(AppShell, minimalProps());

    const toggle = page.getByRole("button", { name: "Open menu" });
    await toggle.click();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "true");

    // A navigation must close the open drawer so the overlay doesn't cover the
    // freshly-routed page.
    router.navigate("/project");
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
  });
});
