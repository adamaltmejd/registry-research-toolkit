import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { CatalogNode } from "./api";
import { getCatalogNode } from "./api";
import CatalogNodeView from "./CatalogNodeView.svelte";

// CatalogNodeView fetches one node via `getCatalogNode(fqidPath)` and switches on
// `kind`. Mock that single GET (mirrors ConceptGroupView's api-mock style); keep
// the rest of api.ts real (the type exports + path helpers `catalog.ts` uses).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogNode: vi.fn(),
  };
});

// A minimal classification-root node with ONE folded umbrella group (`sun`): the
// root carries the flat classification children AND the derived `groups`, and
// `foldGroupedRows` folds the grouped child under the group row. Shaped exactly
// like the wire types (ClassificationRootResponse → ClassificationNode children +
// ConceptGroupSummary groups whose members are ConceptGroupMember).
function classificationRoot(): CatalogNode {
  return {
    kind: "classification-root",
    fqid: "class",
    name: "Classifications",
    children: [
      {
        kind: "classification",
        fqid: "class/sun2020",
        name: "SUN 2020",
        short_name: "sun2020",
      },
    ],
    groups: [
      {
        key: "sun",
        label: "SUN",
        source: "token",
        axes: [{ name: "dimension", label: "dimension" }],
        members: [
          {
            fqid: "class/sun2020",
            name: "SUN 2020",
            facets: [
              { axis: "dimension", value: "niva", label: "Utbildningsnivå" },
            ],
          },
        ],
      },
    ],
  } as unknown as CatalogNode;
}

// A provider node (`scb`) with two register children: `scb/lisa` (named, with a
// purpose blurb) and `scb/lev` (named, null purpose). Shaped like ProviderResponse
// → RegisterNode children — the #806 provider arm renders these as DataTable links
// (name → catalog link) with the FQID code element dropped.
function providerNode(): CatalogNode {
  return {
    kind: "provider",
    fqid: "scb",
    name: "SCB",
    children: [
      {
        kind: "register",
        fqid: "scb/lisa",
        name: "LISA",
        purpose: "Longitudinal integration database",
      },
      {
        kind: "register",
        fqid: "scb/lev",
        name: "LEV",
        purpose: null,
      },
    ],
  } as unknown as CatalogNode;
}

// A register node (`scb/lisa`) with three ungrouped binding-variable children and
// NO `groups` (so `foldGroupedRows` — which tolerates absent groups — yields three
// all-leaf rows). Shaped like RegisterResponse → BindingChild children; the #806
// register arm renders these leaf names in a single-column list.
function registerNode(): CatalogNode {
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    children: [
      { kind: "binding", fqid: "scb/lisa/v1", name: "Alpha" },
      { kind: "binding", fqid: "scb/lisa/v2", name: "Beta" },
      { kind: "binding", fqid: "scb/lisa/v3", name: "Gamma" },
    ],
  } as unknown as CatalogNode;
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
});

describe("CatalogNodeView provider arm", () => {
  it("renders registers as DataTable links with no FQID code element", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(providerNode());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb",
      regMetaVersion: "test",
      steward: "global",
      vintageYear: 2024,
    });

    // #806: each register is a name link to its catalog page…
    await expect
      .element(page.getByRole("link", { name: "LISA" }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    await expect
      .element(page.getByRole("link", { name: "LEV" }))
      .toHaveAttribute("href", "/catalog/scb/lev");
    // …with the purpose blurb shown as the description column.
    await expect
      .element(page.getByText("Longitudinal integration database"))
      .toBeVisible();

    // #806: the raw FQID <code> element is dropped — the link's name is identity.
    expect(container.querySelector("code")).toBeNull();

    // #806: the in-page Breadcrumb nav was removed from every browse arm (the rail
    // owns navigation now). Any arm proves it; assert it here.
    expect(document.querySelector('nav[aria-label="Breadcrumb"]')).toBeNull();
  });

  it("shows EmptyState when the filter matches nothing", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(providerNode());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb",
      regMetaVersion: "test",
      steward: "global",
      vintageYear: 2024,
    });

    const filterBox = page.getByRole("textbox", { name: /Filter registers/i });
    await filterBox.fill("zzznomatch");

    await expect.element(page.getByText(/No registers match/)).toBeVisible();
    expect(container.querySelector('a[href*="scb/lisa"]')).toBeNull();
  });
});

describe("CatalogNodeView register arm", () => {
  it("renders each ungrouped variable on its own row (one per row)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(registerNode());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      vintageYear: 2024,
    });

    // Wait for the leaf rows to render.
    await expect
      .element(page.getByRole("link", { name: "Alpha" }))
      .toBeVisible();

    // The variable list is single-column: forcing the grid to one track stops CSS
    // auto-placement from packing two consecutive ungrouped variables into one
    // visual row (Codex P2). Assert the leaf `.label` spans each occupy a distinct
    // row by their layout top.
    const labels = Array.from(
      container.querySelectorAll<HTMLElement>(".children .label"),
    );
    expect(labels.map((l) => l.textContent)).toEqual([
      "Alpha",
      "Beta",
      "Gamma",
    ]);
    const tops = labels.map((l) => Math.round(l.getBoundingClientRect().top));
    expect(new Set(tops).size).toBe(tops.length);

    // The single-column marker class drives the one-track grid.
    expect(container.querySelector("ul.children.table.single")).not.toBeNull();
  });

  it("makes a variable leaf-row link keyboard-focusable, columns aligned via subgrid (#808 a11y)", async () => {
    // The #808 a11y fix: a leaf row's <a> is a SUBGRID box (NOT display:contents),
    // so it is a real, keyboard-focusable element while its cells still align to the
    // <ul>'s grid tracks. Assert the anchor takes focus (the display:contents version
    // was dropped from Chromium's tab order) AND its columns resolve to `subgrid`.
    vi.mocked(getCatalogNode).mockResolvedValue(registerNode());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("link", { name: "Alpha" }))
      .toBeVisible();

    const link = container.querySelector<HTMLAnchorElement>(
      ".children.table li:not(.group-row) > a",
    );
    expect(link).not.toBeNull();
    const style = getComputedStyle(link as Element);
    expect(style.display).toBe("grid");
    expect(style.gridTemplateColumns).toContain("subgrid");
    // Load-bearing proof: focusing the anchor moves activeElement to it.
    link?.focus();
    expect(document.activeElement).toBe(link);
  });
});

describe("CatalogNodeView classification-root arm (#756)", () => {
  it("renders the umbrella group as a link to its subject page, not an inline <details>", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(classificationRoot());

    await render(CatalogNodeView, {
      fqidPath: "class",
      regMetaVersion: "test",
      steward: "global",
      vintageYear: 2024,
    });

    // #756: the classification umbrella now LINKS to `/catalog/group/class/<key>`
    // (the `.group-link` anchor in ConceptGroupRow's asLink path) — the same flip
    // the register groups got in #673.
    await expect
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/group/class/sun");

    // It must NOT fall back to the old inline disclosure (the pre-#756 behavior).
    expect(document.querySelector("details.group")).toBeNull();
  });

  it("makes a classification leaf-row link keyboard-focusable with name + short_name aligned via subgrid (#808 a11y)", async () => {
    // The two-column classification leaf row: the <a> is a subgrid box spanning both
    // tracks (name in col 1, short_name in col 2), so it's keyboard-focusable AND
    // its cells stay column-aligned. Use a root with an UNGROUPED classification leaf
    // (the grouped one folds into the umbrella group row, which is a separate widget).
    vi.mocked(getCatalogNode).mockResolvedValue({
      kind: "classification-root",
      fqid: "class",
      name: "Classifications",
      children: [
        {
          kind: "classification",
          fqid: "class/atc",
          name: "Anatomical Therapeutic Chemical",
          short_name: "ATC",
        },
      ],
      groups: [],
    } as unknown as CatalogNode);

    await render(CatalogNodeView, {
      fqidPath: "class",
      regMetaVersion: "test",
      steward: "global",
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("link", { name: /Anatomical/ }))
      .toBeVisible();

    const link = document.querySelector<HTMLAnchorElement>(
      ".children.table li:not(.group-row) > a",
    );
    expect(link).not.toBeNull();
    const style = getComputedStyle(link as Element);
    expect(style.display).toBe("grid");
    expect(style.gridTemplateColumns).toContain("subgrid");
    // The short_name cell lands in column 2 (distinct column), not stacked under the
    // name — assert the two cells occupy different horizontal positions.
    const label = link?.querySelector<HTMLElement>(".label");
    const shortName = link?.querySelector<HTMLElement>(".short-name");
    expect(label).not.toBeNull();
    expect(shortName).not.toBeNull();
    const labelLeft = label?.getBoundingClientRect().left ?? 0;
    const shortLeft = shortName?.getBoundingClientRect().left ?? 0;
    expect(shortLeft).toBeGreaterThan(labelLeft);
    // Load-bearing proof: focusing the anchor moves activeElement to it.
    link?.focus();
    expect(document.activeElement).toBe(link);
  });
});
