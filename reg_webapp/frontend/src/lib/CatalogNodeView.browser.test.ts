import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { CatalogNode, StatesResponse, VariableStateModel } from "./api";
import {
  getCatalogNode,
  getClassificationGroup,
  getClassificationGroupGraph,
  getRegisterVariants,
  getRelatedDocuments,
} from "./api";
import CatalogNodeView from "./CatalogNodeView.svelte";
import { projectStore } from "./project_store.svelte";
import {
  datedVersions,
  variant,
  variantsResponse,
} from "./variants-test-helpers";
import { windowStore } from "./window.svelte";

// CatalogNodeView fetches one node via `getCatalogNode(fqidPath)` and switches on
// `kind`. Mock that single GET (mirrors ConceptGroupView's api-mock style); keep
// the rest of api.ts real (the type exports + path helpers `catalog.ts` uses).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
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

/** One `(variant, column)` delivery on a register child (Y-82). `to` null with
 * `open` true is a still-delivered window. */
function delivery(
  variant: string,
  column: string,
  from: string,
  to: string | null,
) {
  return {
    variant,
    column,
    coverage: {
      coverage_from: from,
      coverage_to: to,
      open_ended: to === null,
      state_count: 1,
    },
  };
}

// A register node whose children carry Y-82 `deliveries` — the LISA shape the
// ticket names: `forvink-ers` is delivered as `ForvErs` (a name its slug does not
// contain), `forversnetto` as `ForvErsNetto`, `disp` under TWO columns across a
// rename, and `arbetsstalle` under a SECOND variant. `variants` selects how many
// variants the register is delivered by: one (no chips) or both (chips).
function columnedRegisterNode(variants: 1 | 2): CatalogNode {
  const children = [
    {
      kind: "binding",
      fqid: "scb/lisa/kon",
      name: "Kön",
      deliveries: [delivery("individer-15plus", "Kon", "2018-01-01", null)],
    },
    {
      kind: "binding",
      fqid: "scb/lisa/forvink-ers",
      name: "Förvärvsinkomst",
      deliveries: [
        delivery("individer-15plus", "ForvErs", "1990-01-01", "2021-12-31"),
      ],
    },
    {
      kind: "binding",
      fqid: "scb/lisa/forversnetto",
      name: "Förvärvsinkomst netto",
      deliveries: [
        delivery("individer-15plus", "ForvErsNetto", "2011-01-01", null),
      ],
    },
    {
      kind: "binding",
      fqid: "scb/lisa/disp",
      name: "Disponibel inkomst",
      deliveries: [
        delivery("individer-15plus", "CDISP", "1968-01-01", "2019-12-31"),
        delivery("individer-15plus", "CDISP5", "2020-01-01", null),
      ],
    },
  ];
  if (variants === 2) {
    children.push({
      kind: "binding",
      fqid: "scb/lisa/arbetsstalle",
      name: "Arbetsställe",
      deliveries: [delivery("arbetsstallen", "ArbstNr", "2005-01-01", null)],
    });
  }
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    children,
  } as unknown as CatalogNode;
}

/** The variants the Y-82 fixtures are delivered by, NAMED — the chips read as
 * these words, not as the slugs that key them. */
function lisaVariants() {
  return variantsResponse(
    variant("individer-15plus", { name: "Individer, 15 år och äldre" }),
    variant("individer-16plus", { name: "Individer, 16 år och äldre" }),
    variant("arbetsstallen", { name: "Arbetsställen" }),
  );
}

// One variable delivered under a DIFFERENT column by each variant — the ticket's
// `disp` shape (`CDISP` then `CDISP5`), split so that no single variant ships
// both. `kon` is delivered by both variants under one name, so the register has
// two chips whichever way the lens goes.
function splitColumnRegisterNode(): CatalogNode {
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    children: [
      {
        kind: "binding",
        fqid: "scb/lisa/kon",
        name: "Kön",
        deliveries: [
          delivery("individer-15plus", "Kon", "1990-01-01", null),
          delivery("individer-16plus", "Kon", "1990-01-01", null),
        ],
      },
      {
        kind: "binding",
        fqid: "scb/lisa/disp",
        name: "Disponibel inkomst",
        deliveries: [
          delivery("individer-16plus", "CDISP", "1968-01-01", "2019-12-31"),
          delivery("individer-15plus", "CDISP5", "2020-01-01", null),
        ],
      },
    ],
  } as unknown as CatalogNode;
}

// ONE variable delivered under TWO column names by BOTH variants — the shape a pooled
// variant set cross-authors. `Kon` ticked under one lens and `Alder` under the other
// both exist in both variants, so matching a row by name alone would stage each under
// the other's variant as well. The eras differ, so the committed source periods say
// which column went where.
function crossedColumnRegisterNode(): CatalogNode {
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    children: [
      {
        kind: "binding",
        fqid: "scb/lisa/kon",
        name: "Kön",
        deliveries: [
          delivery("individer-15plus", "Kon", "2010-01-01", null),
          delivery("individer-16plus", "Kon", "2010-01-01", null),
          delivery("individer-15plus", "Alder", "2000-01-01", "2015-12-31"),
          delivery("individer-16plus", "Alder", "2000-01-01", "2015-12-31"),
        ],
      },
    ],
  } as unknown as CatalogNode;
}

// A concept group whose members SPLIT across variants: `kon` + `disp` are
// `individer-15plus`, `arbetsstalle` is `arbetsstallen`, and the ungrouped
// `foretag` is a variant of its own — so a lens can leave the group with two
// members, with one, or with none.
function splitVariantGroupRegisterNode(): CatalogNode {
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    children: [
      {
        kind: "binding",
        fqid: "scb/lisa/kon",
        name: "Kön",
        deliveries: [delivery("individer-15plus", "Kon", "2018-01-01", null)],
      },
      {
        kind: "binding",
        fqid: "scb/lisa/disp",
        name: "Disponibel inkomst",
        deliveries: [delivery("individer-15plus", "CDISP", "1968-01-01", null)],
      },
      {
        kind: "binding",
        fqid: "scb/lisa/arbetsstalle",
        name: "Arbetsställe",
        deliveries: [delivery("arbetsstallen", "ArbstNr", "2005-01-01", null)],
      },
      {
        kind: "binding",
        fqid: "scb/lisa/foretag",
        name: "Företag",
        deliveries: [delivery("foretag", "ForetagNr", "2005-01-01", null)],
      },
    ],
    groups: [
      {
        key: "inkomstbegrepp",
        label: "Inkomstbegrepp",
        source: "token",
        axes: [{ name: "begrepp", label: "Begrepp" }],
        members: [
          {
            fqid: "scb/lisa/kon",
            name: "Kön",
            facets: [{ axis: "begrepp", value: "kon", label: "Kön" }],
          },
          {
            fqid: "scb/lisa/disp",
            name: "Disponibel inkomst",
            facets: [{ axis: "begrepp", value: "disp", label: "Disponibel" }],
          },
          {
            fqid: "scb/lisa/arbetsstalle",
            name: "Arbetsställe",
            facets: [
              { axis: "begrepp", value: "arbst", label: "Arbetsställe" },
            ],
          },
        ],
      },
    ],
  } as unknown as CatalogNode;
}

// `splitColumnRegisterNode`'s `kon` + `disp` FOLDED into one concept-group row
// whose `disp` member is a REPRESENTATION FAMILY (#819) — `disp`'s FQID
// survives under BOTH variants (each delivers it under CDISP or CDISP5), so a
// lens must narrow the group's members by `(fqid, delivery_column)` rather
// than by FQID alone, or the representation the lensed variant does not
// deliver stays in the group (Y-94).
function splitVariantRepresentationGroupRegisterNode(): CatalogNode {
  return {
    ...(splitColumnRegisterNode() as unknown as Record<string, unknown>),
    groups: [
      {
        key: "inkomstbegrepp",
        label: "Inkomstbegrepp",
        source: "token",
        axes: [{ name: "begrepp", label: "Begrepp" }],
        members: [
          {
            fqid: "scb/lisa/kon",
            name: "Kön",
            facets: [{ axis: "begrepp", value: "kon", label: "Kön" }],
          },
          {
            fqid: "scb/lisa/disp",
            name: "Disponibel inkomst",
            delivery_column: "CDISP",
            facets: [{ axis: "begrepp", value: "disp", label: "Disponibel" }],
          },
          {
            fqid: "scb/lisa/disp",
            name: "Disponibel inkomst",
            delivery_column: "CDISP5",
            facets: [{ axis: "begrepp", value: "disp", label: "Disponibel" }],
          },
        ],
      },
    ],
  } as unknown as CatalogNode;
}

// The same register with `kon` + `disp` FOLDED into one concept-group row (#303).
// The row stands in for its members, so the column filter has to reach through it.
function groupedColumnRegisterNode(): CatalogNode {
  return {
    ...(columnedRegisterNode(1) as unknown as Record<string, unknown>),
    groups: [
      {
        key: "inkomstbegrepp",
        label: "Inkomstbegrepp",
        source: "token",
        axes: [{ name: "begrepp", label: "Begrepp" }],
        members: [
          {
            fqid: "scb/lisa/kon",
            name: "Kön",
            facets: [{ axis: "begrepp", value: "kon", label: "Kön" }],
          },
          {
            fqid: "scb/lisa/disp",
            name: "Disponibel inkomst",
            facets: [{ axis: "begrepp", value: "disp", label: "Disponibel" }],
          },
        ],
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

// A register node with MANY ungrouped leaves — enough rows to overflow the test
// viewport vertically (Y-97), the way LISA's ~740-variable list does for a real
// researcher. No `deliveries`, like `registerNode()` above: the sticky-bar proof
// doesn't tick anything, only scrolls past it.
function manyLeavesRegisterNode(count: number): CatalogNode {
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    children: Array.from({ length: count }, (_, i) => ({
      kind: "binding",
      fqid: `scb/lisa/v${i}`,
      name: `Variable ${i}`,
    })),
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
  vi.mocked(getRelatedDocuments).mockResolvedValue({
    kind: "related-documents",
    ingested: true,
    register: "lisa",
    documents: [],
  });
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
  it("keeps an announced classification-shaped placeholder while the route resolves", async () => {
    vi.mocked(getCatalogNode).mockImplementation(() => new Promise(() => {}));

    const { container } = await render(CatalogNodeView, {
      fqidPath: "class/icd-11-se",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    const loading = page.getByText("Loading…", { exact: true });
    await expect.element(loading).toBeVisible();
    const loadingSurface = container.querySelector(".route-loading");
    expect(loadingSurface).toHaveAttribute("aria-busy", "true");
    expect(loadingSurface).toHaveAttribute("aria-live", "polite");
    expect(loadingSurface).toHaveClass("classification-loading");
    expect(container.querySelectorAll(".skeleton.block")).toHaveLength(3);
  });
});

describe("CatalogNodeView provider arm", () => {
  it("renders registers as DataTable links with no FQID code element", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(providerNode());

    const { container } = await render(CatalogNodeView, {
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

    const table = container.querySelector("table.data-table");
    expect(table).not.toBeNull();
    expect(table?.closest(".panel")).toBeNull();
    expect(table?.classList.contains("framed")).toBe(true);
    await expect
      .element(page.getByRole("columnheader", { name: "Register" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("columnheader", { name: "Description" }))
      .toBeVisible();

    // #806: the raw FQID <code> element is dropped — the link's name is identity.
    expect(container.querySelector("code")).toBeNull();

    const lisaLink = container.querySelector<HTMLAnchorElement>(
      'a[href="/catalog/scb/lisa"]',
    );
    const lisaRow = lisaLink?.closest("tr") as HTMLElement | null;
    const descriptionCell = lisaRow?.querySelector(
      "td:nth-child(2)",
    ) as HTMLElement | null;
    let clicks = 0;
    lisaLink?.addEventListener("click", (event) => {
      event.preventDefault();
      clicks += 1;
    });
    expect(table).toHaveAttribute("role", "table");
    expect(lisaRow).not.toHaveAttribute("tabindex");
    descriptionCell?.click();
    expect(clicks).toBe(1);

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
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    const filterBox = page.getByRole("textbox", { name: /Filter registers/i });
    await filterBox.fill("zzznomatch");

    await expect.element(page.getByText(/No registers match/)).toBeVisible();
    expect(container.querySelector('a[href*="scb/lisa"]')).toBeNull();
  });
});

const cellText = (cell: Element | undefined | null): string =>
  (cell?.textContent ?? "").replace(/\s+/g, " ").trim();

/** The rendered variable list as `[variable, its delivery columns]` pairs — the
 * column cell's entries joined, since each is its own line. Scoped to the
 * variable table: the Variants section below renders a DataTable of its own. */
function variableRows(container: Element): [string, string][] {
  const table = [...container.querySelectorAll("table.data-table")].find(
    (t) => !t.closest("section.variants"),
  );
  return [...(table?.querySelectorAll("tbody tr") ?? [])].map((row) => [
    cellText(row.querySelector("td")),
    [...row.querySelectorAll(".delivery-column")].map(cellText).join(", "),
  ]);
}

/** Toggle a variant chip by the NAME it reads as — clicking the chip label, as a
 * pointer does: the checkbox it wraps is visually hidden (present for the keyboard
 * and assistive tech). Waits for the name first: the chips read slugs until the
 * register's variants land. EXACT: the list's own delivery-column ticks (Y-83) are
 * checkboxes too, and a chip's slug is a prefix of a column name often enough
 * (`foretag` / `ForetagNr`) that a substring match is ambiguous. */
async function clickVariantChip(name: string): Promise<void> {
  const checkbox = page.getByRole("checkbox", { name, exact: true });
  await expect.element(checkbox).toBeInTheDocument();
  checkbox.element().closest<HTMLLabelElement>("label")?.click();
}

describe("CatalogNodeView register arm", () => {
  it("renders each ungrouped variable as a framed DataTable row", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(registerNode());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    // Wait for the leaf rows to render.
    await expect
      .element(page.getByRole("link", { name: "Alpha" }))
      .toBeVisible();

    const table = container.querySelector("table.data-table");
    expect(table).not.toBeNull();
    expect(table?.closest(".panel")).toBeNull();
    expect(table?.classList.contains("framed")).toBe(true);
    await expect
      .element(page.getByRole("columnheader", { name: "Variable" }))
      .toBeVisible();
    expect(
      [...container.querySelectorAll("tbody tr")].map((row) =>
        row.textContent?.trim(),
      ),
    ).toEqual(["Alpha", "Beta", "Gamma"]);
  });

  it("names the delivery columns beside each variable, dated only where there are several (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(columnedRegisterNode(1));

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("columnheader", { name: "Delivery column" }))
      .toBeVisible();
    // A one-column variable reads as its column name alone; the two-column one
    // dates each name, so a researcher can tell which era delivers which.
    expect(variableRows(container)).toEqual([
      ["Kön", "Kon"],
      ["Förvärvsinkomst", "ForvErs"],
      ["Förvärvsinkomst netto", "ForvErsNetto"],
      ["Disponibel inkomst", "CDISP 1968–2019, CDISP5 2020–"],
    ]);
  });

  it("filters on delivery column names, case-insensitively (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(columnedRegisterNode(1));

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await page
      .getByRole("textbox", { name: /Filter variables/i })
      .fill("forvers");

    // `forversnetto` matched on its slug before; `forvink-ers` is reachable only
    // through its `ForvErs` column — the miss this ticket is about.
    expect(variableRows(container).map(([name]) => name)).toEqual([
      "Förvärvsinkomst",
      "Förvärvsinkomst netto",
    ]);
    await expect.element(page.getByText("2 of 4")).toBeVisible();
  });

  it("narrows the list to a selected variant's variables (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(columnedRegisterNode(2));
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    // Each chip is a real checkbox, named by the variant's catalog NAME — the
    // word the Variants section below spells it with, not its `?variant=` slug.
    await expect
      .element(page.getByRole("checkbox", { name: "Arbetsställen" }))
      .toBeInTheDocument();
    await expect
      .element(
        page.getByRole("checkbox", { name: "Individer, 15 år och äldre" }),
      )
      .toBeInTheDocument();

    await clickVariantChip("Arbetsställen");

    // With the text box empty FilterInput shows no "x of y", so the chip strip
    // says what it hid — otherwise the list would shrink silently.
    await expect
      .element(page.getByText("Showing 1 of 5 variables"))
      .toBeVisible();
    expect(variableRows(container).map(([name]) => name)).toEqual([
      "Arbetsställe",
    ]);

    // Multi-select: adding the other variant widens the list back out.
    await clickVariantChip("Individer, 15 år och äldre");
    await expect
      .element(page.getByText("Showing 5 of 5 variables"))
      .toBeVisible();
    expect(variableRows(container)).toHaveLength(5);
  });

  it("lifts the variant lens from the chip strip (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(columnedRegisterNode(2));
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    // No lens, no escape hatch — the control appears with the selection.
    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();
    expect(
      page.getByRole("button", { name: "Clear variant filter" }).elements(),
    ).toHaveLength(0);
    // The readout it shares a row with is mounted EMPTY, though: a live region
    // inserted with its first text is never announced.
    expect(
      container.querySelector('[aria-live="polite"]')?.textContent?.trim(),
    ).toBe("");

    await clickVariantChip("Arbetsställen");
    await page.getByRole("button", { name: "Clear variant filter" }).click();

    await expect.element(page.getByRole("link", { name: "Kön" })).toBeVisible();
    expect(variableRows(container)).toHaveLength(5);
    expect(
      container.querySelector<HTMLInputElement>(
        'input[type="checkbox"]:checked',
      ),
    ).toBeNull();
  });

  it("keeps keyboard focus on the chip strip and announces the lift (Y-94)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(columnedRegisterNode(2));
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await clickVariantChip("Arbetsställen");
    const clearButton = page.getByRole("button", {
      name: "Clear variant filter",
    });
    await clearButton.element().focus();
    expect(document.activeElement).toBe(clearButton.element());

    await clearButton.click();

    // The button that had focus just unmounted (`selectedVariants` is empty
    // again) — focus must not have dropped to <body> with it (a6).
    const strip = container.querySelector("fieldset.variant-filter");
    expect(strip).not.toBeNull();
    expect(document.activeElement).not.toBe(document.body);
    expect(strip?.contains(document.activeElement)).toBe(true);
    // …and the readout that went blank alongside it says the lift happened.
    expect(
      container.querySelector('[aria-live="polite"]')?.textContent?.trim(),
    ).toBe("Showing all 5 variables");
  });

  it("says the variant lens is also narrowing an empty result (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(columnedRegisterNode(2));
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();
    await clickVariantChip("Arbetsställen");
    await page.getByRole("textbox", { name: /Filter variables/i }).fill("kon");

    // `kon` matches a variable the OTHER variant delivers, so the miss is the
    // chips' doing as much as the text's — the empty state has to say so and
    // point at the strip that lifts them, not blame the search term alone.
    await expect
      .element(page.getByText("No variables match “kon”"))
      .toBeVisible();
    await expect
      .element(
        page.getByText("The variant filter above is also narrowing this list."),
      )
      .toBeVisible();

    // That strip is still on screen, so the way out is one click from here.
    await page.getByRole("button", { name: "Clear variant filter" }).click();
    expect(variableRows(container).map(([name]) => name)).toEqual(["Kön"]);
  });

  it("reads a variable's columns as the SELECTED variant delivers them (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(splitColumnRegisterNode());
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    // Unlensed, the cell is the whole register: both eras of the rename.
    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();
    expect(variableRows(container)).toEqual([
      ["Kön", "Kon"],
      ["Disponibel inkomst", "CDISP 1968–2019, CDISP5 2020–"],
    ]);

    // Under a lens the cell is that ONE variant's delivery — a researcher reading
    // the list as the variant they will order from must not be shown, and must
    // not be able to order, a column it never delivers.
    await clickVariantChip("Individer, 15 år och äldre");
    expect(variableRows(container)).toEqual([
      ["Kön", "Kon"],
      ["Disponibel inkomst", "CDISP5"],
    ]);

    // …and the filter indexes the same narrowed set: `CDISP5` is not a column
    // `individer-16plus` delivers, so it must not surface `disp` under it.
    await clickVariantChip("Individer, 15 år och äldre");
    await clickVariantChip("Individer, 16 år och äldre");
    expect(variableRows(container)).toEqual([
      ["Kön", "Kon"],
      ["Disponibel inkomst", "CDISP"],
    ]);
    await page
      .getByRole("textbox", { name: /Filter variables/i })
      .fill("cdisp5");
    await expect
      .element(page.getByText("No variables match “cdisp5”"))
      .toBeVisible();
  });

  it("folds a group row over only the members the lens delivers (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      splitVariantGroupRegisterNode(),
    );
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    // Unlensed: the group folds all three of its members.
    await expect
      .element(page.getByRole("link", { name: /Inkomstbegrepp/ }))
      .toBeVisible();
    expect(variableRows(container).map(([name]) => name)).toEqual([
      "Inkomstbegrepp 3 variables",
      "Företag",
    ]);

    // A group row STANDS IN for its members, so the lens has to reach inside it:
    // `arbetsstalle` is gone from the count…
    await clickVariantChip("Individer, 15 år och äldre");
    await expect
      .element(page.getByText("Showing 2 of 4 variables"))
      .toBeVisible();
    expect(variableRows(container).map(([name]) => name)).toEqual([
      "Inkomstbegrepp 2 variables",
    ]);

    // …and from the row's filter keys with it — otherwise the row would still
    // surface for a variable this variant never delivers.
    await page
      .getByRole("textbox", { name: /Filter variables/i })
      .fill("arbetsstalle");
    await expect
      .element(page.getByText("No variables match “arbetsstalle”"))
      .toBeVisible();
  });

  it("drops a group row whose last delivered member the lens takes (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      splitVariantGroupRegisterNode(),
    );
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("link", { name: /Inkomstbegrepp/ }))
      .toBeVisible();

    // `foretag` delivers no member of the group: the row goes with its last one.
    await clickVariantChip("foretag");
    await expect
      .element(page.getByText("Showing 1 of 4 variables"))
      .toBeVisible();
    expect(variableRows(container)).toEqual([["Företag", "ForetagNr"]]);

    // A lens that leaves ONE member leaves no group either — a group row standing
    // in for nobody else is just its member, so the member's own row takes over,
    // delivery column and all.
    await clickVariantChip("foretag");
    await clickVariantChip("Arbetsställen");
    await expect
      .element(page.getByText("Showing 1 of 4 variables"))
      .toBeVisible();
    expect(variableRows(container)).toEqual([["Arbetsställe", "ArbstNr"]]);
  });

  it("narrows a group's representation members by delivery column, not FQID alone (Y-94)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      splitVariantRepresentationGroupRegisterNode(),
    );
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    // Unlensed: `disp`'s two representations dedup to one variable (#819), so
    // the group reads 2 variables (kon + disp) and both columns are findable.
    await expect
      .element(page.getByRole("link", { name: /Inkomstbegrepp/ }))
      .toBeVisible();
    expect(variableRows(container).map(([name]) => name)).toEqual([
      "Inkomstbegrepp 2 variables",
    ]);

    // Lens on the variant that delivers `disp` as CDISP: `disp`'s FQID still
    // survives (it's delivered under CDISP), so the old FQID-only rule would
    // have kept the CDISP5 representation along with it. It must not.
    await clickVariantChip("Individer, 16 år och äldre");
    await page
      .getByRole("textbox", { name: /Filter variables/i })
      .fill("cdisp5");
    await expect
      .element(page.getByText("No variables match “cdisp5”"))
      .toBeVisible();

    // The surviving representation is still findable through the same row.
    await page
      .getByRole("textbox", { name: /Filter variables/i })
      .fill("cdisp");
    await expect
      .element(page.getByRole("link", { name: /Inkomstbegrepp/ }))
      .toBeVisible();
  });

  it("finds a FOLDED variable by its delivery column name (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(groupedColumnRegisterNode());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await page
      .getByRole("textbox", { name: /Filter variables/i })
      .fill("cdisp5");

    // `CDISP5` is `disp`'s SECOND column name — it appears in no slug, fqid or
    // label, so the group row can only surface through its members' columns.
    expect(variableRows(container).map(([name]) => name)).toEqual([
      "Inkomstbegrepp 2 variables",
    ]);
  });

  it("shows no variant chips for a single-variant register (Y-82)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(columnedRegisterNode(1));

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();
    expect(container.querySelector(".variant-filter")).toBeNull();
    // Scoped to the chip strip: the list's own delivery-column ticks (Y-83) are
    // checkboxes, and they are there whether or not the register has a variant axis.
    expect(
      container.querySelectorAll('.variant-filters input[type="checkbox"]'),
    ).toHaveLength(0);
  });

  it("makes a variable leaf-row link keyboard-focusable inside the table cell", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(registerNode());

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("link", { name: "Alpha" }))
      .toBeVisible();

    const link = container.querySelector<HTMLAnchorElement>("tbody td a");
    expect(link).not.toBeNull();
    link?.focus();
    expect(document.activeElement).toBe(link);

    const row = link?.closest("tr") as HTMLElement | null;
    let clicks = 0;
    link?.addEventListener("click", (event) => {
      event.preventDefault();
      clicks += 1;
    });
    expect(row).not.toHaveAttribute("tabindex");
    row?.click();
    expect(clicks).toBe(1);
  });

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
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("heading", { name: "Source documents" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Variants" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "LISA source PDF" }))
      .toHaveAttribute("href", "/api/docs/file/lisa/lisa.pdf");
    const variants = document
      .querySelector("#variants-heading")
      ?.closest("section");
    const sourceDocs = document
      .querySelector("#related-docs-heading")
      ?.closest("section");
    expect(variants).not.toBeNull();
    expect(sourceDocs).not.toBeNull();
    if (!variants || !sourceDocs) {
      throw new Error("Expected variants and source documents sections");
    }
    expect(
      variants.compareDocumentPosition(sourceDocs) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    expect(getRelatedDocuments).toHaveBeenCalledWith("lisa", expect.anything());
  });

  it("summarises the variants instead of the version wall, and links to their page (Y-79)", async () => {
    // The documents-panel-last ordering rides the #967 test above, which asserts
    // it through the same `#variants-heading` anchor this section still carries.
    vi.mocked(getCatalogNode).mockResolvedValue(registerNode());
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        variant("individer-15plus", {
          name: "Individer, 15 år och äldre",
          variant_family: "individer-15plus",
          variant_family_label: "Individer",
          versions: datedVersions(2010),
        }),
        variant("individer-16plus", {
          name: "Individer, 16 år och äldre",
          variant_family: "individer-15plus",
          variant_family_label: "Individer",
          versions: datedVersions(1990),
        }),
      ),
    );

    const { container } = await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    // ONE row for the family: its label, both concrete slugs, its year span.
    await expect
      .element(page.getByRole("heading", { name: "Variants" }))
      .toBeVisible();
    expect(
      container.querySelectorAll("section.variants tbody tr"),
    ).toHaveLength(1);
    await expect.element(page.getByText("1990–2010")).toBeVisible();
    // The version wall no longer renders inline — its prose lives on the page
    // the summary links to.
    expect(container.querySelectorAll("section.version-meta")).toHaveLength(0);
    await expect
      .element(page.getByRole("link", { name: "All variant details" }))
      .toHaveAttribute("href", "/catalog/scb/lisa/variants");
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

// ── Y-83: adding delivery columns straight from the register list ────────────
// The register page is an authoring surface now: each delivery column carries a
// tick, and one action adds every ticked column to the project through the same
// staged add → resolve → commit stack the variable pages use.

/** One state of a variable, as both GETs an Add makes return it: the `?period`
 * resolve (so the committed binding carries a real type) and the per-variable read
 * the exact delivery eras come off. */
function columnState(
  variant: string,
  column: string,
  { id = 1, from = "1990-01-01", to = "9999-12-31" } = {},
): VariableStateModel {
  return {
    state_id: id,
    variant,
    variant_label: null,
    register_variant_id: 1,
    valid_from: from,
    valid_to: to,
    data_type: "int",
    data_length: null,
    delivery_column_name: column,
    source_register_text: null,
    value_set_version_label: "",
    value_set_id: 7,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
  };
}

/** A listed variable's own states, which an Add reads to stage the EXACT delivery
 * eras rather than the list's MIN/MAX aggregate: one state per listed delivery, so
 * a variable's history agrees with the aggregate by default. `eras` gives one
 * variable's column the interrupted history the aggregate cannot express. */
type ColumnedChild = {
  fqid: string;
  deliveries: ReturnType<typeof delivery>[];
};

function columnedLeafNode(
  child: ColumnedChild,
  eras: Record<string, [string, string][]>,
): CatalogNode {
  const states: VariableStateModel[] = [];
  for (const d of child.deliveries) {
    const spans = eras[`${child.fqid}::${d.column}`] ?? [
      [d.coverage.coverage_from, d.coverage.coverage_to ?? "9999-12-31"],
    ];
    for (const [from, to] of spans) {
      states.push(
        columnState(d.variant, d.column, { id: states.length + 1, from, to }),
      );
    }
  }
  return {
    kind: "binding",
    fqid: child.fqid,
    states,
  } as unknown as CatalogNode;
}

/** The browse GET returns the register node; each ticked variable's own GET returns
 * its states, and the staged adds' `?period` GETs resolve to one state each. `hold`
 * keeps the per-variable read pending until it settles, so a case can land something
 * (a New) while an Add's exact-era reads are still in flight. */
function mockRegisterAndResolve(
  node: CatalogNode,
  eras: Record<string, [string, string][]> = {},
  hold?: Promise<void>,
): void {
  const children = (node as unknown as { children: ColumnedChild[] }).children;
  vi.mocked(getCatalogNode).mockImplementation(async (fqid, params) => {
    if (!params?.period) {
      const child = children.find((c) => c.fqid === fqid);
      if (child === undefined) {
        return node;
      }
      await hold;
      return columnedLeafNode(child, eras);
    }
    const variant = typeof params.variant === "string" ? params.variant : "";
    return {
      states: [columnState(variant, fqid.split("/").at(-1) ?? "")],
    } as unknown as StatesResponse;
  });
}

async function renderRegister() {
  return await render(CatalogNodeView, {
    fqidPath: "scb/lisa",
    regMetaVersion: "test",
    steward: "global",
    windowMinYear: 1960,
    vintageYear: 2024,
  });
}

/** Tick a delivery column by the name the list shows it under. */
async function tickColumn(name: string): Promise<void> {
  await page.getByRole("checkbox", { name, exact: true }).click();
}

describe("CatalogNodeView register arm: add columns (Y-83)", () => {
  it("adds every ticked column to the draft in one action", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // Two columns of two DIFFERENT variables, ticked from the list itself — the
    // researcher never opens either variable's page.
    await tickColumn("Kon");
    await tickColumn("ForvErs");
    await expect.element(page.getByText("2 columns selected")).toBeVisible();

    await page
      .getByRole("button", { name: "Add 2 columns to project" })
      .click();

    await expect.element(page.getByText("Applied +2 columns")).toBeVisible();
    // ONE source: both adds land on the same register variant, its period the
    // union of the two window-clipped spans (`Kon` 2018–2023, `ForvErs` 2018–2021).
    expect(projectStore.draft?.sources).toEqual([
      expect.objectContaining({
        register_variant: "scb/lisa/individer-15plus",
        period: { from: 2018, to: 2023 },
        bindings: [
          expect.objectContaining({
            variable: "scb/lisa/kon",
            type: "categorical",
          }),
          expect.objectContaining({
            variable: "scb/lisa/forvink-ers",
            type: "categorical",
          }),
        ],
      }),
    ]);
  });

  it("shows an added column as in the project, unticked, and re-ticking it changes nothing", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    // The tick is consumed and the column now reads as part of the project — the
    // state rides inside the tick's own label, so it is in its accessible name.
    await expect
      .element(
        page.getByRole("checkbox", { name: "Kon In project", exact: true }),
      )
      .not.toBeChecked();
    const committed = JSON.stringify(projectStore.draft);

    // Ticking it again is a no-op: the same add folds into the source it is
    // already in (`applyStagedDiff`'s duplicate-binding guard).
    await tickColumn("Kon In project");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(JSON.stringify(projectStore.draft)).toBe(committed);
  });

  it("refuses an open-ended column with no study window, and says which control fixes it", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // `Kon` is delivered open-ended (2018–). With no study window there is no
    // finite period to commit it under, so the batch is refused whole — and the
    // nudge names the rail's window, the only period control this page has.
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect
      .element(page.getByText(/Apply a period before adding/))
      .toBeVisible();
    await expect
      .element(page.getByText(/set the study window in the rail/))
      .toBeVisible();
    expect(projectStore.draft?.sources).toEqual([]);
    // A refusal the researcher's own next move retires — warn, announced politely
    // (StagedAddStatus, Y-106), never the error tint a read failure gets.
    const refusal = page.getByText(/Apply a period before adding/).element();
    expect(refusal.closest("[role]")?.getAttribute("role")).toBe("status");
  });

  it("retires the study-window nudge once the window is set, keeping the ticks", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect
      .element(page.getByText(/Apply a period before adding/))
      .toBeVisible();

    // Doing what the nudge asked retires it, so the refusal never outlives the
    // pick it refused — and the tick survives, so "add again" is one press.
    windowStore.set({ from: 2018, to: 2023 });
    await expect
      .element(page.getByText(/Apply a period before adding/))
      .not.toBeInTheDocument();
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
  });

  it("clears a stale confirmation when a later Add is refused (Y-106)", async () => {
    // A SECOND register_variant (`arbetsstallen`), carrying no source yet: a
    // pick on `Kon`'s own `individer-15plus` would inherit that source's period
    // instead of refusing, which would test nothing.
    mockRegisterAndResolve(columnedRegisterNode(2));
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();

    // Unsetting the window after a successful Add, then ticking an open-ended
    // column on the OTHER register_variant (`ArbstNr`, delivered 2005–, no
    // existing source) and pressing Add again: that batch has no finite period
    // to commit under and is refused whole. The earlier confirmation must not go
    // on reading as current beside a refusal for a DIFFERENT Add.
    windowStore.set(null);
    await tickColumn("ArbstNr");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect
      .element(page.getByText(/Apply a period before adding/))
      .toBeVisible();
    await expect.element(page.getByText(/Applied/)).not.toBeInTheDocument();
  });

  it("stages what the variant lens shows, one add per delivering variant", async () => {
    mockRegisterAndResolve(splitColumnRegisterNode());
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // `Kon` is delivered by BOTH variants under one name: one tick, one add per
    // concrete register variant (#376), so both sources are authored — and the
    // confirmation still counts the ONE column the researcher ticked.
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      projectStore.draft?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-15plus", "scb/lisa/individer-16plus"]);
  });

  it("stages only the lensed variant of a column two variants deliver", async () => {
    mockRegisterAndResolve(splitColumnRegisterNode());
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // The chip narrows the LIST, and a tick stages exactly what the row now
    // shows: the same `Kon` under ONE variant authors that variant's source
    // alone, where the unlensed tick above authored both.
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      projectStore.draft?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-15plus"]);
  });

  it("keeps a tick scoped to the variant it was made under when the lens is lifted", async () => {
    mockRegisterAndResolve(splitColumnRegisterNode());
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // Ticked while the list showed 15+ ALONE. The lens is a live filter and the tick
    // is not, so lifting it before pressing Add must not hand the batch the 16+
    // delivery of the same name — a variant the researcher never had on screen.
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Clear variant filter" }).click();
    await expect
      .element(page.getByRole("button", { name: "Clear variant filter" }))
      .not.toBeInTheDocument();

    await expect.element(page.getByText("1 column selected")).toBeVisible();
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      projectStore.draft?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-15plus"]);
  });

  it("drops a tick from the batch while the lens shows only a variant it excluded", async () => {
    mockRegisterAndResolve(splitColumnRegisterNode());
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Kon");

    // Moving the lens to the OTHER variant leaves the tick nothing it covers: the
    // `Kon` on screen is now the 16+ delivery, which this tick never stood for. It
    // reads as unticked and there is nothing to add — never a silent switch.
    await clickVariantChip("Individer, 16 år och äldre");
    await clickVariantChip("Individer, 15 år och äldre");
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .not.toBeChecked();
    await expect
      .element(page.getByRole("button", { name: "Add columns to project" }))
      .toBeDisabled();

    // The tick was not thrown away, only held out of the batch: the lens that made
    // it brings it back, still scoped to the one variant it was made under.
    await clickVariantChip("Individer, 16 år och äldre");
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .toBeChecked();
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      projectStore.draft?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-15plus"]);
  });

  it("re-ticking a column under a moved lens captures the variant now on screen", async () => {
    mockRegisterAndResolve(splitColumnRegisterNode());
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Kon");
    await clickVariantChip("Individer, 16 år och äldre");
    await clickVariantChip("Individer, 15 år och äldre");

    // The box reads unticked under this lens, so clicking it must TICK it — and the
    // tick it makes is a tick on what the row shows now, the 16+ delivery.
    await tickColumn("Kon");
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .toBeChecked();
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      projectStore.draft?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-16plus"]);
  });

  it("stages each column under the variants ITS OWN tick was made under", async () => {
    mockRegisterAndResolve(crossedColumnRegisterNode());
    vi.mocked(getRegisterVariants).mockResolvedValue(lisaVariants());
    windowStore.set({ from: 1990, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // Two columns of ONE variable, ticked under DIFFERENT lenses. Both names exist in
    // both variants, so a batch that pooled the two ticks' variants would stage each
    // column under the other's variant too — four adds where the researcher made two.
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Kon 2010–");
    await clickVariantChip("Individer, 16 år och äldre");
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Alder 2000–2015");
    // Only `Alder` is in the batch here — `Kon`'s tick covers no variant this lens
    // shows — which is what makes the two ticks' variant sets genuinely different.
    await expect.element(page.getByText("1 column selected")).toBeVisible();
    await page.getByRole("button", { name: "Clear variant filter" }).click();

    await expect.element(page.getByText("2 columns selected")).toBeVisible();
    await page
      .getByRole("button", { name: "Add 2 columns to project" })
      .click();
    await expect.element(page.getByText("Applied +2 columns")).toBeVisible();

    // `Kon` under 15+ alone and `Alder` under 16+ alone: one source each, over that
    // column's own era. Pooled, both sources would span BOTH eras instead.
    expect(
      Object.fromEntries(
        (projectStore.draft?.sources ?? []).map((source) => [
          source.register_variant,
          source.period,
        ]),
      ),
    ).toEqual({
      "scb/lisa/individer-15plus": { from: 2010, to: 2023 },
      "scb/lisa/individer-16plus": { from: 2000, to: 2015 },
    });
  });

  it("commits an interrupted column as its real eras, not the list's aggregate span", async () => {
    // The list reads `ForvErs` as 1990–2021, the MIN/MAX over its deliveries. The
    // variable's own states say it was delivered 1990–1999 and again 2010–2021,
    // with a decade in between that it was not.
    mockRegisterAndResolve(columnedRegisterNode(1), {
      "scb/lisa/forvink-ers::ForvErs": [
        ["1990-01-01", "1999-12-31"],
        ["2010-01-01", "2021-12-31"],
      ],
    });
    windowStore.set({ from: 1995, to: 2015 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("ForvErs");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    // The gap years are carved out as the #307 list form — what the variable's own
    // page commits, and NOT the single 1995–2015 span the aggregate would give.
    expect(projectStore.draft?.sources[0]?.period).toEqual([
      { from: 1995, to: 1999 },
      { from: 2010, to: 2015 },
    ]);
  });

  it("commits a sequentially renamed column as the ONE row its variable's page commits", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    windowStore.set({ from: 2015, to: 2022 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // `disp` is delivered as CDISP through 2019 and as CDISP5 from 2020 — a
    // sequential RENAME, which the list still lists under both names (Y-82: the name
    // is what a researcher hunts for) where the variable's own page shows the ONE
    // folded row (#902).
    await tickColumn("CDISP 1968–2019");
    await tickColumn("CDISP5 2020–");
    await expect.element(page.getByText("2 columns selected")).toBeVisible();

    await page
      .getByRole("button", { name: "Add 2 columns to project" })
      .click();

    // Both ticked columns are confirmed — that is what the researcher ticked — and
    // they commit as ONE binding over the window-clipped union, exactly as the leaf
    // page commits its folded row. Never one pinned binding per column name: pinning
    // either would break the other's era.
    await expect.element(page.getByText("Applied +2 columns")).toBeVisible();
    expect(projectStore.draft?.sources).toEqual([
      expect.objectContaining({
        register_variant: "scb/lisa/individer-15plus",
        period: { from: 2015, to: 2022 },
        bindings: [
          expect.objectContaining({
            variable: "scb/lisa/disp",
            representation: null,
          }),
        ],
      }),
    ]);
  });

  it("commits the whole rename when only one of its names is ticked", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    windowStore.set({ from: 2015, to: 2022 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // One tick of the RETIRED name is still a pick of the variable's one folded row:
    // it commits the same 2015–2022 the variable's own page commits, where per-period
    // resolution reads CDISP through 2019 and CDISP5 after it. A period stopping at
    // 2019 would be a row this variable has not had since #902 folded it.
    await tickColumn("CDISP 1968–2019");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(projectStore.draft?.sources[0]?.period).toEqual({
      from: 2015,
      to: 2022,
    });
    // The list's other name for it reads as in the project too — one representation,
    // both of its names.
    await expect
      .element(
        page.getByRole("checkbox", {
          name: "CDISP5 2020– In project",
          exact: true,
        }),
      )
      .not.toBeChecked();
  });

  it("refuses the whole batch when a ticked variable's states can't be read", async () => {
    const node = columnedRegisterNode(1);
    vi.mocked(getCatalogNode).mockImplementation(async (fqid) => {
      if (fqid === "scb/lisa") {
        return node;
      }
      throw new Error("offline");
    });
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    // Without the states the exact eras are unknown, and the list's aggregate may
    // claim years the column was never delivered in — so nothing is authored.
    await expect
      .element(page.getByText(/Could not read the delivery years/))
      .toBeVisible();
    expect(projectStore.draft?.sources).toEqual([]);
    // A read that failed, not a refusal: the error tint and `alert` (StagedAddStatus,
    // Y-106) — nobody declined anything, the batch just couldn't be evaluated.
    const failure = page.getByText(/Could not read the delivery years/).element();
    expect(failure.closest("[role]")?.getAttribute("role")).toBe("alert");
  });

  it("refuses a column the study window has moved off, on its own row", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    // The list shows every column the register ever delivered. `ForvErs` ended in
    // 2021, so under a 2022–2024 window it has no era to commit and no period to
    // commit it under — the study window is this page's only period control.
    windowStore.set({ from: 2022, to: 2024 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // The reason rides inside the tick's own label, so it is in its accessible
    // name — and the tick itself is not offered.
    const forvErs = page.getByRole("checkbox", {
      name: "ForvErs Not delivered in 2022–2024",
      exact: true,
    });
    await expect.element(forvErs).toBeDisabled();
    await expect.element(forvErs).not.toBeChecked();
    // Nothing is staged, so nothing can be reported as applied and no project is
    // minted for an empty diff.
    await expect
      .element(page.getByRole("button", { name: "Add columns to project" }))
      .toBeDisabled();
    expect(projectStore.draft?.sources).toEqual([]);
  });

  it("counts only the columns that committed when the real eras miss the window", async () => {
    // The list reads `ForvErs` as 1990–2021, so a 2000–2005 window passes the tick.
    // Its real eras stop in 1999 and resume in 2010, and the window falls in the gap
    // — where `CDISP` (1968–2019) really is delivered.
    mockRegisterAndResolve(columnedRegisterNode(1), {
      "scb/lisa/forvink-ers::ForvErs": [
        ["1990-01-01", "1999-12-31"],
        ["2010-01-01", "2021-12-31"],
      ],
    });
    windowStore.set({ from: 2000, to: 2005 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("ForvErs");
    await tickColumn("CDISP 1968–2019");
    await expect.element(page.getByText("2 columns selected")).toBeVisible();

    await page
      .getByRole("button", { name: "Add 2 columns to project" })
      .click();

    // The confirmation counts what was authored, not what was ticked: the aggregate
    // the bar could see said two, the states said one — and names the column the
    // real eras dropped, so the researcher sees what did not commit.
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    await expect
      .element(page.getByText(/ForvErs not delivered in 2000–2005/))
      .toBeVisible();
    expect(projectStore.draft?.sources).toEqual([
      expect.objectContaining({
        register_variant: "scb/lisa/individer-15plus",
        period: { from: 2000, to: 2005 },
        bindings: [expect.objectContaining({ variable: "scb/lisa/disp" })],
      }),
    ]);
    // The dropped column's OWN tick survives — never cleared by a batch it took
    // no part in — so an Add under a window that covers it is one press.
    await expect
      .element(page.getByRole("checkbox", { name: "ForvErs", exact: true }))
      .toBeChecked();
    // The committed column's tick IS consumed, and now reads as in the project.
    await expect
      .element(
        page.getByRole("checkbox", {
          name: "CDISP 1968–2019 In project",
          exact: true,
        }),
      )
      .not.toBeChecked();
  });

  it("authors nothing when the real eras miss the window for every ticked column", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1), {
      "scb/lisa/forvink-ers::ForvErs": [
        ["1990-01-01", "1999-12-31"],
        ["2010-01-01", "2021-12-31"],
      ],
    });
    windowStore.set({ from: 2000, to: 2005 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("ForvErs");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    // Nothing left to commit once the aggregate gave way to the states, so the batch
    // is refused whole rather than committing the span it was never delivered over.
    await expect
      .element(page.getByText(/No ticked column was delivered in 2000–2005/))
      .toBeVisible();
    expect(projectStore.draft?.sources).toEqual([]);
  });

  it("abandons a batch whose study window moves while the eras are read", async () => {
    let releaseStates = (): void => {};
    mockRegisterAndResolve(
      columnedRegisterNode(1),
      {},
      new Promise<void>((resolve) => {
        releaseStates = resolve;
      }),
    );
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .toBeVisible();

    // The rail's window is not disabled by an Add, and it IS this page's period:
    // moving it mid-read makes the pending batch a pick under years the researcher
    // has already left, so it is abandoned rather than committed as 2018–2023.
    windowStore.set({ from: 2019, to: 2023 });
    releaseStates();

    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .not.toBeInTheDocument();
    expect(projectStore.draft?.sources).toEqual([]);
    // Abandoned, not refused — and the tick survives, so the Add the new window
    // asks for is one press.
    await expect.element(page.getByText(/Applied/)).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .toBeChecked();
  });

  it("abandons a batch whose project is replaced while the eras are read", async () => {
    let releaseStates = (): void => {};
    // The per-variable read an Add makes, held open so a New can land while the batch
    // is still in flight.
    mockRegisterAndResolve(
      columnedRegisterNode(1),
      {},
      new Promise<void>((resolve) => {
        releaseStates = resolve;
      }),
    );
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .toBeVisible();

    // A New replaces the project the pick was staged against — appending to the
    // replacement would corrupt a document the researcher never picked from.
    projectStore.newProject({
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
    });
    releaseStates();

    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .not.toBeInTheDocument();
    expect(projectStore.draft?.sources).toEqual([]);
    // Abandoned, not refused: nothing was authored and nothing is claimed either
    // way, and the tick survives for an Add against the project now open.
    await expect.element(page.getByText(/Applied/)).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .toBeChecked();
  });

  it("abandons a batch whose page changes while the eras are read (Y-106)", async () => {
    let releaseStates = (): void => {};
    // The per-variable read an Add makes, held open so a navigation can land while
    // the batch is still in flight.
    mockRegisterAndResolve(
      columnedRegisterNode(1),
      {},
      new Promise<void>((resolve) => {
        releaseStates = resolve;
      }),
    );
    windowStore.set({ from: 2018, to: 2023 });

    const screen = await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .toBeVisible();

    // This component is REUSED register → register (the route-reset `$effect`),
    // so `unmounted` alone would miss a page change mid-read: the batch must bind
    // to the route too, or the read completing would report on the register the
    // researcher has since opened.
    await screen.rerender({ fqidPath: "scb/rams" });
    releaseStates();

    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .not.toBeInTheDocument();
    expect(projectStore.draft?.sources).toEqual([]);
    // Nothing is authored and nothing is reported — not a refusal either — on the
    // page the researcher has since opened.
    await expect.element(page.getByText(/Applied/)).not.toBeInTheDocument();
    await expect
      .element(page.getByText(/Could not read the delivery years/))
      .not.toBeInTheDocument();
  });

  it("keeps keyboard focus on the Add button through an add (Y-106)", async () => {
    let releaseStates = (): void => {};
    // A mixed batch (as the dropped-column test above): `ForvErs` stays ticked
    // after the Add, so the button still has work to do and stays genuinely
    // enabled afterward too — the focus check isn't confounded by the button
    // later, separately, disabling because nothing is left to add.
    mockRegisterAndResolve(
      columnedRegisterNode(1),
      {
        "scb/lisa/forvink-ers::ForvErs": [
          ["1990-01-01", "1999-12-31"],
          ["2010-01-01", "2021-12-31"],
        ],
      },
      new Promise<void>((resolve) => {
        releaseStates = resolve;
      }),
    );
    windowStore.set({ from: 2000, to: 2005 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("ForvErs");
    await tickColumn("CDISP 1968–2019");

    const addButton = page.getByRole("button", {
      name: "Add 2 columns to project",
    });
    const addButtonEl = addButton.element();
    await addButton.click();
    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .toBeVisible();

    // `aria-disabled` freezes the ACTION while the add is in flight; the element
    // itself stays in the tab order, so the press that started it keeps focus
    // instead of dropping to <body> — a natively `disabled` button's fate.
    expect(document.activeElement).toBe(addButtonEl);

    releaseStates();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(document.activeElement).toBe(addButtonEl);
  });
});

describe("CatalogNodeView register arm: sticky add bar (Y-97)", () => {
  it("keeps the add bar pinned to the viewport bottom after scrolling through a long list", async () => {
    await page.viewport(1280, 800);
    vi.mocked(getCatalogNode).mockResolvedValue(manyLeavesRegisterNode(60));

    await renderRegister();
    await expect.element(page.getByText("Variable 0")).toBeVisible();

    // Scroll well past the bar's unscrolled position under the table — the
    // "two thirds down a long list" the ticket's researcher is in.
    window.scrollTo(0, document.documentElement.scrollHeight / 2);

    const bar = document.querySelector<HTMLElement>(".add-bar");
    expect(bar).not.toBeNull();
    const rect = bar?.getBoundingClientRect();
    const viewportBottom = window.innerHeight;
    // Pinned flush to the viewport's bottom edge (within 1px)…
    const bottomGap = Math.abs((rect?.bottom ?? 0) - viewportBottom);
    expect(bottomGap).toBeLessThanOrEqual(1);
    // …and its top is on screen, not clipped above the fold.
    expect(rect?.top ?? -1).toBeGreaterThanOrEqual(0);
    expect(rect?.top ?? viewportBottom).toBeLessThan(viewportBottom);

    // The page itself never grows a horizontal scrollbar — DataTable's own
    // wrapper owns the (unused, here narrow) horizontal overflow, not `.routed`.
    expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(
      document.documentElement.clientWidth + 1,
    );
  });
});

describe("CatalogNodeView classification-root arm (#756)", () => {
  it("renders the umbrella group as a link to its subject page, not an inline <details>", async () => {
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

    expect(document.querySelector("a.group-link .group-key")).toBeNull();
    const table = document.querySelector("table.data-table");
    expect(table).not.toBeNull();
    expect(table?.closest(".panel")).not.toBeNull();
    expect(table?.closest(".classification-table")).not.toBeNull();
    await expect
      .element(page.getByRole("heading", { name: "Classifications" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Classification systems" }))
      .toBeVisible();
    expect(document.body.textContent).not.toContain("Catalog-wide index");
    const tableHead = table?.querySelector("thead");
    expect(tableHead).not.toBeNull();
    expect(getComputedStyle(tableHead as Element).position).toBe("absolute");

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
      windowMinYear: 1960,
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

    const shortNameCell = row?.querySelector(
      "td:last-child",
    ) as HTMLElement | null;
    let clicks = 0;
    link?.addEventListener("click", (event) => {
      event.preventDefault();
      clicks += 1;
    });
    expect(row).not.toHaveAttribute("tabindex");
    shortNameCell?.click();
    expect(clicks).toBe(1);
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

  it("keeps family value-set geometry stable while the edition graph resolves", async () => {
    const editions = [
      {
        slug: "icd-10-se",
        fqid: "class/icd-10-se",
        name: "ICD-10-SE",
        version_year: 1997,
        is_current: true,
      },
      {
        slug: "icd-11-se",
        fqid: "class/icd-11-se",
        name: "ICD-11-SE",
        version_year: 2027,
        is_current: false,
      },
    ];
    let resolveGraph: (value: never) => void = () => {};
    vi.mocked(getClassificationGroupGraph).mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveGraph = resolve;
        }),
    );
    vi.mocked(getCatalogNode).mockResolvedValue({
      kind: "classification",
      fqid: "class/icd-11-se",
      name: "ICD-11-SE",
      short_name: "ICD-11-SE",
      edition_chain: [],
      codes: [{ code: "1A", label: "Infection", level: 1, is_valid: true }],
      dimensions: [],
      family: {
        kind: "classification-family",
        key: "icd",
        label: "ICD",
        editions,
      },
      derived_from: [],
      derivatives: [],
    } as unknown as CatalogNode);
    vi.mocked(getClassificationGroup).mockResolvedValue({
      kind: "classification-family",
      key: "icd",
      label: "ICD",
      editions,
    } as never);

    await render(CatalogNodeView, {
      fqidPath: "class/icd-11-se",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2027,
    });

    const valueSet = page.getByRole("region", { name: "Value set" });
    await expect.element(valueSet).toBeVisible();
    const before = valueSet.element().getBoundingClientRect().top;

    resolveGraph({
      nodes: [
        ...editions.map((edition) => ({
          kind: "classification" as const,
          id: edition.slug,
          fqid: edition.fqid,
          label: edition.name,
          group_key: "icd",
          version_year: edition.version_year,
          is_current: edition.is_current,
        })),
      ],
      edges: [
        {
          id: "icd-succession",
          kind: "succession",
          source: "icd-10-se",
          target: "icd-11-se",
          label: null,
          effective_year: 2027,
        },
      ],
      focus_id: null,
    } as never);

    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
    const after = valueSet.element().getBoundingClientRect().top;
    expect(Math.abs(after - before)).toBeLessThan(2);
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
    expect(getCatalogNode).toHaveBeenLastCalledWith("class/ssyk1996");
  });
});
