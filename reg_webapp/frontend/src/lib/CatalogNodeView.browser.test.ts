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
        axes: ["dimension"],
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
});
