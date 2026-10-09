import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { CatalogNode } from "./api";
import {
  getCatalogNode,
  getClassificationGroup,
  getClassificationGroupGraph,
  getRegisterVariants,
  getRelatedDocuments,
} from "./api";
import CatalogNodeView from "./CatalogNodeView.svelte";
import { projectStore } from "./project_store.svelte";
import { variant, variantsResponse } from "./variants-test-helpers";
import { windowStore } from "./window.svelte";

// CatalogNodeView fetches one node via `getCatalogNode(fqidPath)` and switches on
// `kind`. Mock that single GET (mirrors ConceptGroupView's api-mock style); keep
// the rest of api.ts real (the type exports + path helpers `catalog.ts` uses).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getDataWarnings: vi.fn().mockResolvedValue([]),
    getCatalogNode: vi.fn(),
    getClassificationGroup: vi.fn(),
    getClassificationGroupGraph: vi.fn(),
    getRegisterVariants: vi.fn(),
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

function classificationRootWithFamily(): CatalogNode {
  return {
    kind: "classification-root",
    fqid: "class",
    name: "Classifications",
    children: [],
    groups: [],
    families: [
      {
        kind: "classification-family",
        key: "ssyk",
        label: "SSYK",
        editions: [
          {
            slug: "ssyk1996",
            fqid: "class/ssyk1996",
            name: "SSYK 1996",
            effective_year: 2012,
            version_year: 1996,
            is_current: false,
            is_self: false,
          },
          {
            slug: "ssyk2012",
            fqid: "class/ssyk2012",
            name: "SSYK 2012",
            effective_year: null,
            version_year: 2012,
            is_current: true,
            is_self: false,
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
// all-leaf rows). Its children carry no `deliveries` either, which the register arm
// must tolerate the same way (a payload from before the field existed).
// Shaped like RegisterResponse → BindingChild children.
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
  vi.mocked(getClassificationGroup).mockReset();
  vi.mocked(getClassificationGroupGraph).mockReset();
  vi.mocked(getRegisterVariants).mockReset();
  vi.mocked(getRelatedDocuments).mockReset();
  vi.mocked(getClassificationGroupGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  });
  vi.mocked(getRegisterVariants).mockResolvedValue(
    variantsResponse(variant("_default")),
  );
  vi.mocked(getRelatedDocuments).mockResolvedValue([]);
  // Both stores are module singletons: clear the browse-time window fallback, then
  // open a fresh empty draft — the state a catalog page authors into. A fresh draft
  // seeds its window from that (now empty) fallback, so each case starts windowless.
  windowStore.set(null);
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

describe("CatalogNodeView loading geometry", () => {
  it("announces a busy loading region while the route resolves", async () => {
    vi.mocked(getCatalogNode).mockImplementation(() => new Promise(() => {}));

    await render(CatalogNodeView, {
      fqidPath: "class/icd-11-se",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    const loading = page.getByText("Loading…", { exact: true });
    await expect.element(loading).toBeVisible();
    const region = loading.element().closest("[aria-busy]");
    expect(region).toHaveAttribute("aria-busy", "true");
    expect(region).toHaveAttribute("aria-live", "polite");
  });
});

describe("CatalogNodeView provider arm", () => {
  it("lists registers as links with their purpose under Register and Description", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(providerNode());

    await render(CatalogNodeView, {
      fqidPath: "scb",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
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

    await expect
      .element(page.getByRole("columnheader", { name: "Register" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("columnheader", { name: "Description" }))
      .toBeVisible();
  });
});

describe("CatalogNodeView register arm", () => {
  it("renders thematic tags on the register page", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(registerNode());

    await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect.element(page.getByText("Income & earnings")).toBeVisible();
  });

  it("renders register-grain source documents on register pages (#967)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(registerNode());
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(variant("combined", { name: "Combined register" })),
    );
    vi.mocked(getRelatedDocuments).mockResolvedValue([
      {
        title: "LISA source PDF",
        filename: "lisa.pdf",
        source_url: "https://www.scb.se/lisa",
        license: "CC BY 4.0",
        fetched: "2026-06-01",
        sha256: "a".repeat(64),
        byte_size: 1024,
      },
    ]);

    await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    const sourceDocs = page.getByRole("heading", { name: "Source documents" });
    const variants = page.getByRole("heading", { name: "Variants" });
    await expect.element(sourceDocs).toBeVisible();
    await expect.element(variants).toBeVisible();
    expect(
      variants.element().compareDocumentPosition(sourceDocs.element()) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    // The panel is keyed by the register FQID: fails if the page passes the bare
    // slug (`/api/docs/file/lisa/…`), which the Rust server does not route.
    await expect
      .element(page.getByRole("link", { name: "LISA source PDF" }))
      .toHaveAttribute("href", "/api/docs/file/scb/lisa/lisa.pdf");
  });

  it("renders grouped variables as framed table subject links without the group-key pill", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(groupedRegisterNode());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("link", { name: /Inkomst per månad/ }))
      .toHaveAttribute("href", "/catalog/group/scb/lisa/ink");
    await expect.element(page.getByText("2 variables")).toBeVisible();

    const groupLink = container.querySelector("a.group-link");
    expect(groupLink?.closest("table.data-table")).not.toBeNull();
    expect(
      groupLink?.closest("table.data-table")?.classList.contains("framed"),
    ).toBe(true);
    expect(groupLink?.closest(".panel")).toBeNull();
    expect(groupLink?.querySelector(".group-key")).toBeNull();
  });
});

describe("CatalogNodeView classification-root arm (#756)", () => {
  it("renders the umbrella group as a link to its subject page", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(classificationRoot());

    await render(CatalogNodeView, {
      fqidPath: "class",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    // #756: the classification umbrella now LINKS to `/catalog/group/class/<key>`
    // (the `.group-link` anchor in ConceptGroupRow's asLink path) — the same flip
    // the register groups got in #673.
    await expect
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/group/class/sun");

    await expect
      .element(page.getByRole("heading", { name: "Classifications" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Classification systems" }))
      .toBeVisible();
    // The table head is visually hidden but stays in the accessibility tree.
    expect(page.getByRole("columnheader").elements().length).toBeGreaterThan(0);
  });

  it("renders a classification leaf as a link plus its short name", async () => {
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
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("link", { name: /Anatomical/ }))
      .toBeVisible();

    await expect.element(page.getByText("ATC", { exact: true })).toBeVisible();
  });

  it("renders a classification succession family as a stable concept link", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(classificationRootWithFamily());

    await render(CatalogNodeView, {
      fqidPath: "class",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("link", { name: "SSYK (2 editions)" }))
      .toHaveAttribute("href", "/catalog/group/class/ssyk");
    await expect.element(page.getByText("2 editions")).toBeVisible();
    await expect.element(page.getByText("ssyk2012")).toBeVisible();
  });

  it("renders a grouped classification FQID through the canonical group tabs", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue({
      kind: "classification",
      fqid: "class/sun2020",
      name: "SUN 2020",
      short_name: "SUN2020",
      edition_chain: [],
      codes: [{ code: "1", label: "Man", level: 1, is_valid: true }],
      dimensions: [
        {
          key: "sun",
          label: "Svensk utbildningsnomenklatur",
          source: "token",
          axes: [],
          members: [
            {
              fqid: "class/sun2020",
              name: "SUN 2020",
              facets: [{ axis: null, value: "niva", label: "Utbildningsnivå" }],
            },
          ],
        },
      ],
      family: null,
      derived_from: [],
      derivatives: [],
    } as unknown as CatalogNode);
    vi.mocked(getClassificationGroup).mockResolvedValue({
      kind: "classification-group",
      key: "sun",
      label: "Svensk utbildningsnomenklatur",
      source: "token",
      axes: [],
      members: [
        {
          fqid: "class/sun2020",
          name: "SUN 2020",
          facets: [{ axis: null, value: "niva", label: "Utbildningsnivå" }],
        },
      ],
    });

    await render(CatalogNodeView, {
      fqidPath: "class/sun2020",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect
      .element(
        page.getByRole("heading", {
          name: "Classification group: Svensk utbildningsnomenklatur",
        }),
      )
      .toBeVisible();
    await expect
      .element(page.getByRole("tab", { name: /Utbildningsnivå/ }))
      .toHaveAttribute("aria-selected", "true");
    await expect.element(page.getByText("Man")).toBeVisible();
  });

  it("opens classification family aliases on the canonical self edition tab", async () => {
    vi.mocked(getCatalogNode)
      .mockResolvedValueOnce({
        kind: "classification",
        fqid: "class/ssyk1996-legacy",
        name: "SSYK 1996",
        short_name: "SSYK1996",
        edition_chain: [
          {
            slug: "ssyk1996",
            fqid: "class/ssyk1996",
            name: "SSYK 1996",
            effective_year: 2012,
            version_year: 1996,
            is_current: false,
            is_self: true,
          },
          {
            slug: "ssyk2012",
            fqid: "class/ssyk2012",
            name: "SSYK 2012",
            effective_year: null,
            version_year: 2012,
            is_current: true,
            is_self: false,
          },
        ],
        codes: [
          { code: "1", label: "Older occupation", level: 1, is_valid: true },
        ],
        dimensions: [],
        family: {
          kind: "classification-family",
          key: "ssyk",
          label: "SSYK",
          editions: [
            {
              slug: "ssyk1996",
              fqid: "class/ssyk1996",
              name: "SSYK 1996",
              effective_year: 2012,
              version_year: 1996,
              is_current: false,
              is_self: true,
            },
            {
              slug: "ssyk2012",
              fqid: "class/ssyk2012",
              name: "SSYK 2012",
              effective_year: null,
              version_year: 2012,
              is_current: true,
              is_self: false,
            },
          ],
        },
        derived_from: [],
        derivatives: [],
      } as unknown as CatalogNode)
      .mockResolvedValueOnce({
        kind: "classification",
        fqid: "class/ssyk1996",
        name: "SSYK 1996",
        short_name: "SSYK1996",
        edition_chain: [],
        codes: [
          { code: "1", label: "Older occupation", level: 1, is_valid: true },
        ],
        dimensions: [],
        family: null,
        derived_from: [],
        derivatives: [],
      } as unknown as CatalogNode);
    vi.mocked(getClassificationGroup).mockResolvedValue({
      kind: "classification-family",
      key: "ssyk",
      label: "SSYK",
      editions: [
        {
          slug: "ssyk1996",
          fqid: "class/ssyk1996",
          name: "SSYK 1996",
          short_name: "SSYK1996",
          effective_year: 2012,
          version_year: 1996,
          is_current: false,
          is_self: true,
        },
        {
          slug: "ssyk2012",
          fqid: "class/ssyk2012",
          name: "SSYK 2012",
          short_name: "SSYK2012",
          effective_year: null,
          version_year: 2012,
          is_current: true,
          is_self: false,
        },
      ],
    });

    await render(CatalogNodeView, {
      fqidPath: "class/ssyk1996-legacy",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("tab", { name: /SSYK 1996/ }))
      .toHaveAttribute("aria-selected", "true");
    await expect
      .element(page.getByRole("tab", { name: /SSYK 2012/ }))
      .toHaveAttribute("aria-selected", "false");
    await expect.element(page.getByText("Older occupation")).toBeVisible();
  });
});
