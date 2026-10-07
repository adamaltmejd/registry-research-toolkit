// Split from ConceptGroupView.browser.test.ts by contract surface: member navigation and ?member= focus.
// Siblings: ConceptGroupView{,.selection,.labels,.navigation,.filters,.succession}.browser.test.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { getCatalogNode, getConceptGroup, getConceptGroupGraph } from "./api";
import {
  graph,
  mockResolveColumns,
  node,
  renderGroup,
  twoSingleColGraph,
} from "./concept-group-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogNode: vi.fn(),
    getConceptGroup: vi.fn(),
    getConceptGroupGraph: vi.fn(),
  };
});

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  mockResolveColumns({});
  vi.mocked(getConceptGroup).mockReset();
  vi.mocked(getConceptGroupGraph).mockReset();
  // Default: an empty graph (overridden per case).
  vi.mocked(getConceptGroupGraph).mockResolvedValue(graph([]));
  router.navigate("/catalog/group/scb/rams/ink");
  windowStore.set(null);
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

describe("ConceptGroupView (#617 + #678 compact column list)", () => {
  // ── Member → leaf navigation (#678) ─────────────────────────────────────────
  it("a single-column member's COLUMN CHIP is the leaf-navigation link (no separate 'View' link)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    await renderGroup();

    // The column chip ITSELF is the navigation link to the member's leaf FQID — there
    // is no separate "View ↗" link anymore.
    const janLink = await vi.waitFor(() => {
      const els = [...document.querySelectorAll("a.col-chip.link")];
      const jan = els.find(
        (a) => a.getAttribute("href") === "/catalog/scb/rams/inkjan",
      );
      if (!jan) {
        throw new Error("inkjan column-chip link not yet rendered");
      }
      return jan;
    });
    expect(janLink.tagName).toBe("A");
    // The chip-link is inside the row label (the click-anywhere selection target) but
    // is itself a real <a> (keyboard-navigable; it stops propagation so a nav click
    // never toggles).
    expect(janLink.closest("label.row-btn")).not.toBeNull();
    // The other member's chip links too.
    expect(
      document.querySelector(
        'a.col-chip.link[href="/catalog/scb/rams/inkfeb"]',
      ),
    ).not.toBeNull();
    // No legacy "View ↗" link survives.
    expect(document.querySelector("a.open-link")).toBeNull();
  });

  // ── #678 finding 5: the member link carries the active group ?period ─────────
  it("a member nav link carries the active group ?period", async () => {
    router.navigate("/catalog/group/scb/rams/ink?period=2018..2020");
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    await renderGroup();

    // The chip link keeps the window the user narrowed the group to.
    const janLink = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.col-chip.link[href*="/catalog/scb/rams/inkjan"]',
      );
      if (!el) {
        throw new Error("inkjan chip link not yet rendered");
      }
      return el;
    });
    expect(janLink.getAttribute("href")).toBe(
      "/catalog/scb/rams/inkjan?period=2018..2020",
    );
  });

  // ── #678 finding 6: chip nav goes through the SPA router (no full reload) ─────
  it("a plain chip click routes in-app without toggling the row", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    await renderGroup();

    const janLink = page.getByRole("link", { name: /^Inkjan/ });
    await expect
      .element(janLink)
      .toHaveAttribute("href", "/catalog/scb/rams/inkjan");

    // A plain left click is prevented (no full reload) and routed in-app, and it does
    // not toggle the row's selection.
    const plain = new MouseEvent("click", {
      bubbles: true,
      cancelable: true,
      button: 0,
    });
    janLink.element().dispatchEvent(plain);
    expect(plain.defaultPrevented).toBe(true);
    expect(location.pathname).toBe("/catalog/scb/rams/inkjan");
    await expect
      .element(page.getByRole("checkbox", { name: /Inkjan/ }))
      .not.toBeChecked();
  });

  it("a modifier chip click is left to the browser", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    await renderGroup();

    const janLink = page.getByRole("link", { name: /^Inkjan/ });
    await expect
      .element(janLink)
      .toHaveAttribute("href", "/catalog/scb/rams/inkjan");
    const groupPath = location.pathname;

    // A modifier click is NOT prevented by the component (open-in-new-tab intent).
    // An un-prevented click on a real <a href> would navigate the test iframe, so a
    // document-level probe — registered AFTER Svelte's delegated handler — records
    // whether the component prevented it, then prevents the real navigation.
    let componentPrevented = true;
    const probe = (e: Event) => {
      componentPrevented = e.defaultPrevented;
      e.preventDefault();
    };
    document.addEventListener("click", probe);
    try {
      janLink.element().dispatchEvent(
        new MouseEvent("click", {
          bubbles: true,
          cancelable: true,
          button: 0,
          metaKey: true,
        }),
      );
    } finally {
      document.removeEventListener("click", probe);
    }
    expect(componentPrevented).toBe(false);
    expect(location.pathname).toBe(groupPath);
  });
});

describe("ConceptGroupView ?member= focus highlight (#678 finding 5)", () => {
  it("marks the band the validated ?member= hint names", async () => {
    // The backend echoes the validated focus slug on `node.member`; the band keyed by
    // the member fqid whose leaf slug is that slug gets the focus marker.
    vi.mocked(getConceptGroup).mockResolvedValue(node({ member: "inkfeb" }));
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    router.navigate("/catalog/group/scb/rams/ink?member=inkfeb");

    await renderGroup();

    const focused = await vi.waitFor(() => {
      const el = document.querySelector(".col-row.single.focused");
      if (!el) {
        throw new Error("focused band not yet rendered");
      }
      return el;
    });
    // Exactly the inkfeb band is focused (not inkjan).
    expect(document.querySelectorAll(".focused")).toHaveLength(1);
    expect(focused.textContent).toContain("Inkfeb");
  });

  it("keeps a focused successor navigable after succession folds to list mode", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node({ member: "inkfeb" }));
    vi.mocked(getConceptGroupGraph).mockResolvedValue({
      ...twoSingleColGraph(),
      edges: [
        {
          id: "succession:scb/rams/inkjan->scb/rams/inkfeb",
          kind: "succession",
          source: "scb/rams/inkjan",
          target: "scb/rams/inkfeb",
          label: null,
          effective_year: 2018,
        },
      ],
    });
    router.navigate("/catalog/group/scb/rams/ink?member=inkfeb");

    await renderGroup();

    const focusedLink = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        '.col-row.single.focused a.col-chip[href="/catalog/scb/rams/inkfeb"]',
      );
      if (!el) {
        throw new Error("focused member link not yet rendered");
      }
      return el;
    });
    expect(focusedLink.getAttribute("href")).toBe("/catalog/scb/rams/inkfeb");
    expect(document.querySelector(".graph-picker")).toBeNull();
  });
});
