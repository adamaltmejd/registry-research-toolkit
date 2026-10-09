import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ClassificationGroupShow,
  ClassificationRootShow,
  ClassificationShow,
  ProviderShow,
  RegisterShow,
  ShowNode,
  VariableShow,
} from "./api";
import {
  ApiError,
  getGraph,
  getRelatedDocuments,
  getShow,
  getStates,
  getValues,
} from "./api";
import CatalogNodeView from "./CatalogNodeView.svelte";
import { registerShow, variableChild } from "./catalog-node-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { variant } from "./variants-test-helpers";
import { windowStore } from "./window.svelte";

// CatalogNodeView reads one node via `getShow(fqidPath)` and switches on `kind`;
// the views it delegates to read their own facets (`getGraph`, `getValues`,
// `getWarnings`). Mock those GETs; keep the rest of api.ts real (the type exports +
// path helpers `catalog.ts` uses).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getShow: vi.fn(),
    getStates: vi.fn(),
    getWarnings: vi.fn().mockResolvedValue([]),
    getGraph: vi.fn(),
    getValues: vi.fn(),
    getRelatedDocuments: vi.fn(),
  };
});

/** `getShow` answers each ref from `nodes` and 404s the rest. */
function mockShow(nodes: Record<string, ShowNode>): void {
  vi.mocked(getShow).mockImplementation(async (ref) => {
    const node = ref === undefined ? undefined : nodes[ref];
    if (node === undefined) {
      throw new ApiError(404, null, `not found: ${ref}`);
    }
    return node;
  });
}

function renderNode(fqidPath: string) {
  return render(CatalogNodeView, {
    fqidPath,
    regMetaVersion: "test",
    steward: "global",
    windowMinYear: 1960,
    vintageYear: 2024,
  });
}

// A minimal classification-root node with ONE folded umbrella group (`sun`): the
// root carries the flat classification children AND the derived `groups`, and
// `foldGroupedRows` folds the grouped child under the group row.
function classificationRoot(): ClassificationRootShow {
  return {
    kind: "classification_root",
    fqid: "class",
    name: "Classifications",
    families: [],
    children: [
      { fqid: "class/sun2020", name: "SUN 2020", short_name: "sun2020" },
    ],
    groups: [
      {
        fqid: "group/class/sun",
        key: "sun",
        label: "SUN",
        source: "token",
        tags: [],
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
  };
}

function classificationRootWithFamily(): ClassificationRootShow {
  return {
    kind: "classification_root",
    fqid: "class",
    name: "Classifications",
    children: [],
    groups: [],
    families: [
      {
        fqid: "group/class/ssyk",
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
  };
}

// A provider node (`scb`) with two register children: `scb/lisa` (named, with a
// purpose blurb) and `scb/lev` (named, null purpose) — the #806 provider arm
// renders these as DataTable links (name → catalog link).
function providerNode(): ProviderShow {
  return {
    kind: "provider",
    fqid: "scb",
    name: "SCB",
    children: [
      {
        fqid: "scb/lisa",
        name: "LISA",
        purpose: "Longitudinal integration database",
        tags: [],
      },
      { fqid: "scb/lev", name: "LEV", purpose: null, tags: [] },
    ],
  };
}

// A register node (`scb/lisa`) with three ungrouped variable children and no
// groups, so `foldGroupedRows` yields three all-leaf rows.
function registerNode(fields: Partial<RegisterShow> = {}): RegisterShow {
  return registerShow({
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
      variableChild("scb/lisa/v1", "Alpha"),
      variableChild("scb/lisa/v2", "Beta"),
      variableChild("scb/lisa/v3", "Gamma"),
    ],
    ...fields,
  });
}

function groupedRegisterNode(): RegisterShow {
  return registerShow({
    children: [
      variableChild("scb/lisa/inkjan", "Inkomst januari"),
      variableChild("scb/lisa/inkfeb", "Inkomst februari"),
      variableChild("scb/lisa/kon", "Kön"),
    ],
    groups: [
      {
        fqid: "group/scb/lisa/ink",
        key: "ink",
        label: "Inkomst per månad",
        source: "token",
        tags: [],
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
  });
}

function variableNode(): VariableShow {
  return {
    kind: "variable",
    fqid: "scb/lisa/kon",
    name: "Kön",
    deprecated: false,
    is_identifier: false,
    is_sensitive: false,
    same_as: [],
    tags: [],
  };
}

beforeEach(() => {
  vi.mocked(getShow).mockReset();
  vi.mocked(getStates).mockReset();
  vi.mocked(getGraph).mockReset();
  vi.mocked(getValues).mockReset();
  vi.mocked(getRelatedDocuments).mockReset();
  vi.mocked(getGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  });
  vi.mocked(getValues).mockResolvedValue({
    items: [],
    next_cursor: null,
    total: 0,
  });
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
    vi.mocked(getShow).mockImplementation(() => new Promise(() => {}));

    await renderNode("class/icd-11-se");

    const loading = page.getByText("Loading…", { exact: true });
    await expect.element(loading).toBeVisible();
    const region = loading.element().closest("[aria-busy]");
    expect(region).toHaveAttribute("aria-busy", "true");
    expect(region).toHaveAttribute("aria-live", "polite");
  });
});

describe("CatalogNodeView variable arm", () => {
  // Fails if the page renders a variable before (or without) its state history
  // read, or swallows that read's failure: the states are read with `show`, as one
  // load with one error surface.
  it("reports a failed state-history read as the page's error", async () => {
    // A retired ref: `show` answers its successor, and the states are read by
    // that canonical fqid, not by the route path.
    mockShow({ "scb/lisa/kon-old": variableNode() });
    vi.mocked(getStates).mockRejectedValue(
      new ApiError(500, null, "states unavailable"),
    );

    await renderNode("scb/lisa/kon-old");

    await expect
      .element(page.getByRole("alert"))
      .toHaveTextContent("states unavailable");
    expect(vi.mocked(getStates).mock.calls).toEqual([["scb/lisa/kon"]]);
  });
});

describe("CatalogNodeView provider arm", () => {
  it("lists registers as links with their purpose under Register and Description", async () => {
    mockShow({ scb: providerNode() });

    await renderNode("scb");

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
    mockShow({ "scb/lisa": registerNode() });

    await renderNode("scb/lisa");

    await expect.element(page.getByText("Income & earnings")).toBeVisible();
  });

  // Fails if the Variants section stops reading the register node's own
  // `variants` (there is no separate variants read any more).
  it("renders register-grain source documents on register pages (#967)", async () => {
    mockShow({
      "scb/lisa": registerNode({
        variants: [variant("combined", { name: "Combined register" })],
      }),
    });
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

    await renderNode("scb/lisa");

    const sourceDocs = page.getByRole("heading", { name: "Source documents" });
    const variants = page.getByRole("heading", { name: "Variants" });
    await expect.element(sourceDocs).toBeVisible();
    await expect.element(variants).toBeVisible();
    await expect.element(page.getByText("Combined register")).toBeVisible();
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
    mockShow({ "scb/lisa": groupedRegisterNode() });

    const { container } = await renderNode("scb/lisa");

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
  // Fails if the arm stops matching the `classification_root` kind.
  it("renders the umbrella group as a link to its subject page", async () => {
    mockShow({ class: classificationRoot() });

    await renderNode("class");

    // #756: the classification umbrella LINKS to `/catalog/group/class/<key>`
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
    mockShow({
      class: {
        kind: "classification_root",
        fqid: "class",
        name: "Classifications",
        children: [
          {
            fqid: "class/atc",
            name: "Anatomical Therapeutic Chemical",
            short_name: "ATC",
          },
        ],
        groups: [],
        families: [],
      },
    });

    await renderNode("class");

    await expect
      .element(page.getByRole("link", { name: /Anatomical/ }))
      .toBeVisible();

    await expect.element(page.getByText("ATC", { exact: true })).toBeVisible();
  });

  it("renders a classification succession family as a stable concept link", async () => {
    mockShow({ class: classificationRootWithFamily() });

    await renderNode("class");

    await expect
      .element(page.getByRole("link", { name: "SSYK (2 editions)" }))
      .toHaveAttribute("href", "/catalog/group/class/ssyk");
    await expect.element(page.getByText("2 editions")).toBeVisible();
    await expect.element(page.getByText("ssyk2012")).toBeVisible();
  });

  // Fails if a grouped classification stops opening its group page on the
  // node's own (canonical) fqid tab, or the subject key stops reading the first
  // dimension when the classification has no family.
  it("renders a grouped classification FQID through the canonical group tabs", async () => {
    const sunMembers = [
      {
        fqid: "class/sun2020",
        name: "SUN 2020",
        facets: [{ axis: null, value: "niva", label: "Utbildningsnivå" }],
      },
    ];
    const classification: ClassificationShow = {
      kind: "classification",
      fqid: "class/sun2020",
      name: "SUN 2020",
      short_name: "SUN2020",
      dimensions: [
        {
          fqid: "group/class/sun",
          key: "sun",
          label: "Svensk utbildningsnomenklatur",
          source: "token",
          tags: [],
          axes: [],
          members: sunMembers,
        },
      ],
      family: null,
      derived_from: [],
      derivatives: [],
      variables: [],
    };
    const group: ClassificationGroupShow = {
      kind: "classification_group",
      fqid: "group/class/sun",
      key: "sun",
      label: "Svensk utbildningsnomenklatur",
      source: "token",
      axes: [],
      members: sunMembers,
    };
    mockShow({
      "class/sun2020": classification,
      "group/class/sun": group,
    });
    vi.mocked(getValues).mockResolvedValue({
      items: [{ code: "1", label: "Man", level: 1, is_valid: true }],
      next_cursor: null,
      total: 1,
    });

    await renderNode("class/sun2020");

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
});
