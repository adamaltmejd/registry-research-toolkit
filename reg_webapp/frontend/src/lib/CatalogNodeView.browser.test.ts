import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { CatalogNode } from "./api";
import { getCatalogNode, getRelatedDocuments } from "./api";
import CatalogNodeView from "./CatalogNodeView.svelte";

// CatalogNodeView fetches one node via `getCatalogNode(fqidPath)` and switches on
// `kind`. Mock that single GET (mirrors ConceptGroupView's api-mock style); keep
// the rest of api.ts real (the type exports + path helpers `catalog.ts` uses).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogNode: vi.fn(),
    getRelatedDocuments: vi.fn(),
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
    tags: [
      {
        slug: "income",
        label: "Income & earnings",
        rank: 1,
        starred: false,
        note: null,
      },
    ],
    children: [
      { kind: "binding", fqid: "scb/lisa/v1", name: "Alpha" },
      { kind: "binding", fqid: "scb/lisa/v2", name: "Beta" },
      { kind: "binding", fqid: "scb/lisa/v3", name: "Gamma" },
    ],
  } as unknown as CatalogNode;
}

function groupedRegisterNode(): CatalogNode {
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    children: [
      { kind: "binding", fqid: "scb/lisa/inkjan", name: "Inkomst januari" },
      { kind: "binding", fqid: "scb/lisa/inkfeb", name: "Inkomst februari" },
      { kind: "binding", fqid: "scb/lisa/kon", name: "Kön" },
    ],
    groups: [
      {
        key: "ink",
        label: "Inkomst per månad",
        source: "token",
        axes: [{ name: "month", label: "month" }],
        members: [
          {
            fqid: "scb/lisa/inkjan",
            name: "Inkomst januari",
            facets: [{ axis: "month", value: "01", label: "januari" }],
          },
          {
            fqid: "scb/lisa/inkfeb",
            name: "Inkomst februari",
            facets: [{ axis: "month", value: "02", label: "februari" }],
          },
        ],
      },
    ],
  } as unknown as CatalogNode;
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getRelatedDocuments).mockReset();
  vi.mocked(getRelatedDocuments).mockResolvedValue({
    kind: "related-documents",
    ingested: true,
    register: "lisa",
    documents: [],
  });
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

    const table = container.querySelector("table.data-table");
    expect(table).not.toBeNull();
    expect(table?.closest(".panel")).not.toBeNull();

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
  it("renders each ungrouped variable as a DataTable row inside a Panel", async () => {
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

    const table = container.querySelector("table.data-table");
    expect(table).not.toBeNull();
    expect(table?.closest(".panel")).not.toBeNull();
    expect(
      [...container.querySelectorAll("tbody tr")].map((row) =>
        row.textContent?.trim(),
      ),
    ).toEqual(["Alpha", "Beta", "Gamma"]);
  });

  it("makes a variable leaf-row link keyboard-focusable inside the table cell", async () => {
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

    const link = container.querySelector<HTMLAnchorElement>("tbody td a");
    expect(link).not.toBeNull();
    link?.focus();
    expect(document.activeElement).toBe(link);
  });

  it("renders thematic tags on the register page", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(registerNode());

    await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      vintageYear: 2024,
    });

    await expect.element(page.getByText("Income & earnings")).toBeVisible();
  });

  it("renders register-grain source documents on register pages (#967)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(registerNode());
    vi.mocked(getRelatedDocuments).mockResolvedValue({
      kind: "related-documents",
      ingested: true,
      register: "lisa",
      documents: [
        {
          title: "LISA source PDF",
          filename: "lisa.pdf",
          source_url: "https://www.scb.se/lisa",
          license: "CC BY 4.0",
          fetched: "2026-06-01",
          sha256: "a".repeat(64),
          byte_size: 1024,
        },
      ],
    });

    await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("heading", { name: "Source documents" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "LISA source PDF" }))
      .toHaveAttribute("href", "/api/docs/file/lisa/lisa.pdf");
    expect(getRelatedDocuments).toHaveBeenCalledWith("lisa", expect.anything());
  });

  it("renders grouped variables as subject links in the Panel without the group-key pill", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(groupedRegisterNode());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("link", { name: /Inkomst per månad/ }))
      .toHaveAttribute("href", "/catalog/group/scb/lisa/ink");
    await expect.element(page.getByText("2 variables")).toBeVisible();

    const groupLink = container.querySelector("a.group-link");
    expect(groupLink?.closest("table.data-table")).not.toBeNull();
    expect(groupLink?.closest(".panel")).not.toBeNull();
    expect(groupLink?.querySelector(".group-key")).toBeNull();
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

    expect(document.querySelector("a.group-link .group-key")).toBeNull();
    const table = document.querySelector("table.data-table");
    expect(table).not.toBeNull();
    expect(table?.closest(".panel")).not.toBeNull();
    await expect
      .element(page.getByRole("heading", { name: "Catalog-wide index" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("columnheader", { name: "Name", exact: true }))
      .toBeVisible();

    // It must NOT fall back to the old inline disclosure (the pre-#756 behavior).
    expect(document.querySelector("details.group")).toBeNull();
  });

  it("renders a classification leaf as a focusable link plus short_name column", async () => {
    // Use a root with an UNGROUPED classification leaf (the grouped one folds into
    // the umbrella group row, which is a separate widget).
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

    const link = document.querySelector<HTMLAnchorElement>("tbody td a");
    expect(link).not.toBeNull();
    const row = link?.closest("tr");
    expect(row?.querySelectorAll("td")).toHaveLength(2);
    expect(row?.querySelector("td:last-child")?.textContent?.trim()).toBe(
      "ATC",
    );
    link?.focus();
    expect(document.activeElement).toBe(link);
  });
});
