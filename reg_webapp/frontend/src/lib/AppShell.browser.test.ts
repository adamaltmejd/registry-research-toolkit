import { createRawSnippet } from "svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page, userEvent } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import AppShell from "./AppShell.svelte";
import type { RootResponse } from "./api";
import { getCatalogRoot } from "./api";
import { DATA_BROWSER_LABEL } from "./catalog";
import type { ProjectData } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { link, router } from "./router.svelte";

// Stub ONLY the catalog-root GET (the shell's provider-facet fetch); keep the
// rest of api.ts real — mirrors SearchOmnibox's partial-mock pattern (override
// the GET, leave types + helpers intact).
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
    steward: { long_name: "Statistiska Centralbyrån", id: "scb" },
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
  vi.unstubAllGlobals();
});

// The browser-test viewport is 414px — below the 48rem drawer breakpoint — so the
// persistent rail is `display: none` and the drawer is the only rail. The drawer
// is a Bits UI Dialog: it is in the document ONLY while it is open. So any test
// asserting rail content (facets / primary nav) must OPEN it first, exercising
// the only state where the rail is reachable on mobile. Desktop keeps the rail
// always visible (the media query doesn't apply there).
//
// The desktop rail is still in the DOM here, just `display: none`. Role queries
// skip it; a bare text query would match both copies, so ask the drawer.
const menuToggle = () => page.getByRole("button", { name: "Open menu" });
const drawer = () => page.getByRole("dialog", { name: "Menu" });

async function openDrawer(): Promise<void> {
  await menuToggle().click();
}

describe("AppShell — provider facets", () => {
  it("renders each provider as a link inside the Providers nav", async () => {
    await render(AppShell, minimalProps());
    await openDrawer();

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
    await openDrawer();

    await expect
      .element(page.getByRole("link", { name: DATA_BROWSER_LABEL }))
      .toHaveAttribute("aria-current", "page");
    // The Project link is NOT current.
    expect(
      page.getByRole("link", { name: /^Project:/ }).query(),
    ).not.toHaveAttribute("aria-current");
  });

  it("marks the Project link current on the project route (inverse)", async () => {
    setUrl("/project");
    await render(AppShell, minimalProps());
    await openDrawer();

    await expect
      .element(page.getByRole("link", { name: /^Project:/ }))
      .toHaveAttribute("aria-current", "page");
    expect(
      page.getByRole("link", { name: DATA_BROWSER_LABEL }).query(),
    ).not.toHaveAttribute("aria-current");
  });
});

describe("AppShell — project chip", () => {
  it("shows the no-project state in the drawer", async () => {
    await render(AppShell, minimalProps());
    await openDrawer();

    const chip = page.getByRole("link", { name: /^Project: No project/ });
    await expect.element(chip).toBeVisible();
    await expect.element(chip).toHaveAttribute("href", "/project");
    await expect
      .element(drawer().getByText("0 sources · 0 columns"))
      .toBeVisible();
    await expect.element(drawer().getByText("Unchecked")).toBeVisible();
  });

  it("shows project name, counts, and validation status from the store", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({
        ok: true,
        status: 200,
        json: async () => ({
          ok: true,
          issues: [{ level: "warning", code: "w", path: "", message: "note" }],
        }),
      })),
    );
    projectStore.newProject({
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
    });
    projectStore.updateField("name", "Cancer sibling study");
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/v1",
          period: 2018,
          binding: { variable: "scb/lisa/kon", type: "categorical" },
        },
        {
          registerVariant: "scb/lisa/v1",
          period: 2018,
          binding: { variable: "scb/lisa/alder", type: "numeric" },
        },
        {
          registerVariant: "scb/rtb/v1",
          period: 2019,
          binding: { variable: "scb/rtb/fodelsear", type: "numeric" },
        },
      ],
    });
    await projectStore.validate();

    await render(AppShell, minimalProps());
    await openDrawer();

    await expect
      .element(
        page.getByRole("link", { name: /^Project: Cancer sibling study/ }),
      )
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /unsaved changes/i }))
      .toBeVisible();
    await expect
      .element(drawer().getByText("2 sources · 3 columns"))
      .toBeVisible();
    await expect.element(drawer().getByText("Warnings")).toBeVisible();
  });

  it("names the completed check in the chip when the draft validates clean", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({
        ok: true,
        status: 200,
        json: async () => ({ ok: true, issues: [] }),
      })),
    );
    projectStore.newProject({
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
    });
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/v1",
          period: 2018,
          binding: { variable: "scb/lisa/kon", type: "categorical" },
        },
      ],
    });
    await projectStore.validate();

    await render(AppShell, minimalProps());
    await openDrawer();

    // The panel's vocabulary, not a bare "Valid": the chip rides along on every
    // route, including one showing a blocked order.
    await expect.element(drawer().getByText("Draft valid")).toBeVisible();
  });

  it("tolerates malformed source slots while showing counts", async () => {
    // The two halves of an Open: the file ingress, then the commit (the toolbar
    // puts the replacement confirmation between them).
    const parsed = projectStore.parseProjectText(
      JSON.stringify({
        schema_version: "2.0.0",
        steward: "global",
        reg_meta_version: "reg_meta/v1.0.0",
        name: "Malformed project",
        sources: [
          null,
          {
            name: "LISA",
            register_variant: "scb/lisa/v1",
            period: 2018,
            bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
          },
        ],
      }),
    );
    expect(parsed).not.toBeNull();
    projectStore.loadProject(parsed as ProjectData);

    await render(AppShell, minimalProps());
    await openDrawer();

    await expect
      .element(page.getByRole("link", { name: /^Project: Malformed project/ }))
      .toBeVisible();
    await expect
      .element(drawer().getByText("2 sources · 1 column"))
      .toBeVisible();
  });
});

describe("AppShell — mobile drawer", () => {
  it("opens on the hamburger and closes on a pointer outside it", async () => {
    const { container } = await render(AppShell, minimalProps());

    const toggle = menuToggle();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
    // The drawer exists only while it is open.
    expect(drawer().query()).toBeNull();

    await toggle.click();
    await expect.element(drawer()).toBeVisible();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "true");

    // The dimmer covers the exposed content beside the drawer, and that is where
    // a dismissing pointer lands — the tap-out the hand-written scrim button did.
    const scrim = container.querySelector(".drawer-scrim");
    expect(scrim).not.toBeNull();
    await page.elementLocator(scrim as Element).click();

    await expect.element(drawer()).not.toBeInTheDocument();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
  });

  it("opens from the keyboard, contains focus, and hands it back on Escape", async () => {
    await render(AppShell, minimalProps());

    const toggle = menuToggle();
    toggle.element().focus();
    await userEvent.keyboard("{Enter}");
    await expect.element(drawer()).toBeVisible();
    // Wait for the facets so the drawer holds its full set of controls before
    // the tab traversal below counts on them.
    await expect
      .element(drawer().getByRole("link", { name: "sos" }))
      .toBeVisible();

    const panel = drawer().element();
    // Focus ENTERS the drawer — it does not stay on the toggle the drawer covers.
    await vi.waitFor(() => {
      expect(panel.contains(document.activeElement)).toBe(true);
    });

    // The background controls the ticket names — the toggle under the drawer and
    // the catalog search beside it — are outside the panel, so "every Tab stop is
    // inside the panel" is exactly the claim that Tab never reaches them.
    const search = page.getByRole("textbox", { name: "Search the catalog" });
    expect(panel.contains(toggle.element())).toBe(false);
    expect(panel.contains(search.element())).toBe(false);

    // Tab past the last of the drawer's stops: every one is a drawer control,
    // and the traversal wraps rather than escaping into the page behind.
    const visited = new Set<Element>();
    for (let i = 0; i < 8; i++) {
      await userEvent.keyboard("{Tab}");
      const active = document.activeElement;
      expect(panel.contains(active)).toBe(true);
      if (active != null) {
        visited.add(active);
      }
    }
    // The traversal really moved — a trap that pinned focus to one control would
    // satisfy the containment assertions above but not this.
    expect(visited.size).toBeGreaterThan(1);

    await userEvent.keyboard("{Escape}");

    await expect.element(drawer()).not.toBeInTheDocument();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
    await vi.waitFor(() => {
      expect(document.activeElement).toBe(toggle.element());
    });
  });

  it("closes when the route changes underneath it", async () => {
    await render(AppShell, minimalProps());

    const toggle = menuToggle();
    await toggle.click();
    await expect.element(drawer()).toBeVisible();

    // Not every navigation comes from one of the drawer's own links — browser
    // back/forward moves the route with the drawer standing, and it must not be
    // left covering the page that arrives.
    router.navigate("/project");

    await expect.element(drawer()).not.toBeInTheDocument();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
  });

  it("closes when a drawer link points at the route already showing", async () => {
    setUrl("/catalog");
    const { container } = await render(AppShell, minimalProps());
    link(container as HTMLElement);

    await openDrawer();
    await drawer().getByRole("link", { name: DATA_BROWSER_LABEL }).click();

    // The router no-ops a navigation to the URL already showing, so the route
    // never changes — and the drawer still has to get out of the way of the page
    // the researcher just asked for.
    expect(window.location.pathname).toBe("/catalog");
    expect(router.route.name).toBe("root");
    await expect.element(drawer()).not.toBeInTheDocument();
    await expect
      .element(menuToggle())
      .toHaveAttribute("aria-expanded", "false");
  });

  it("closes when the viewport grows past the drawer breakpoint", async () => {
    // Pin the precondition: this suite renders below the 48rem breakpoint, where
    // the drawer IS the rail (mirrors SourceEditor/SearchView).
    expect(window.matchMedia("(max-width: 48rem)").matches).toBe(true);
    const mobile = { width: window.innerWidth, height: window.innerHeight };

    try {
      await render(AppShell, minimalProps());
      await openDrawer();
      await expect.element(drawer()).toBeVisible();
      // The dialog is modal: it locks the page behind it.
      expect(getComputedStyle(document.body).overflow).toBe("hidden");
      expect(getComputedStyle(document.body).pointerEvents).toBe("none");

      await page.viewport(1280, 900);
      await vi.waitFor(() => {
        expect(window.matchMedia("(max-width: 48rem)").matches).toBe(false);
      });

      // Above the breakpoint the persistent rail is the rail, so the drawer must
      // not stand over it — and closing it is what releases the modal
      // restrictions the dialog put on the page behind.
      await expect.element(drawer()).not.toBeInTheDocument();
      await expect
        .element(page.getByRole("complementary", { name: "Primary" }))
        .toBeVisible();
      await vi.waitFor(() => {
        // The page behind is usable again — the modal held the body's scrolling
        // AND its pointer events, and a close that kept either would leave the
        // desktop layout dead.
        expect(getComputedStyle(document.body).overflow).not.toBe("hidden");
        expect(getComputedStyle(document.body).pointerEvents).not.toBe("none");
      });

      // Back below the breakpoint: the drawer starts closed rather than
      // reappearing where it was left.
      await page.viewport(mobile.width, mobile.height);
      await vi.waitFor(() => {
        expect(window.matchMedia("(max-width: 48rem)").matches).toBe(true);
      });
      expect(drawer().query()).toBeNull();
      await expect
        .element(menuToggle())
        .toHaveAttribute("aria-expanded", "false");
    } finally {
      // A failed assertion above must not leak a desktop viewport into the
      // cases that follow.
      await page.viewport(mobile.width, mobile.height);
    }
  });

  it("closes when a drawer link navigates (the close-on-navigate $effect)", async () => {
    const { container } = await render(AppShell, minimalProps());
    // The shell relies on App's `use:link` root to pushState-route its links;
    // rendered on its own it has no such ancestor, so give it one — otherwise a
    // facet click leaves the page instead of routing.
    link(container as HTMLElement);

    await openDrawer();
    await drawer().getByRole("link", { name: "sos" }).click();

    // A navigation must close the drawer so it doesn't cover the freshly routed
    // page, and focus must come back to the control that opened it.
    expect(router.route.name).toBe("catalog-node");
    await expect.element(drawer()).not.toBeInTheDocument();
    await expect
      .element(menuToggle())
      .toHaveAttribute("aria-expanded", "false");
    await vi.waitFor(() => {
      expect(document.activeElement).toBe(menuToggle().element());
    });
  });
});

describe("AppShell — viewport geometry", () => {
  it("grows the main canvas through the remaining viewport with short routed content", async () => {
    const { container } = await render(AppShell, minimalProps());

    const shell = container.querySelector<HTMLElement>(".shell");
    const frame = container.querySelector<HTMLElement>(".frame");
    const topbar = container.querySelector<HTMLElement>(".topbar");
    const canvas = container.querySelector<HTMLElement>(".canvas");
    expect(shell).not.toBeNull();
    expect(frame).not.toBeNull();
    expect(topbar).not.toBeNull();
    expect(canvas).not.toBeNull();

    const shellRect = shell?.getBoundingClientRect();
    const frameRect = frame?.getBoundingClientRect();
    const topbarRect = topbar?.getBoundingClientRect();
    const canvasRect = canvas?.getBoundingClientRect();
    expect(shellRect?.height).toBeGreaterThanOrEqual(window.innerHeight);
    expect(frameRect?.bottom).toBeCloseTo(shellRect?.bottom ?? 0, 0);
    expect(canvasRect?.top).toBeCloseTo(topbarRect?.bottom ?? 0, 0);
    expect(canvasRect?.bottom).toBeCloseTo(frameRect?.bottom ?? 0, 0);
  });
});
