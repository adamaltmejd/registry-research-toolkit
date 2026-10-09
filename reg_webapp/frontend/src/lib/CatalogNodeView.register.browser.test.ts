// Split from CatalogNodeView.browser.test.ts by contract surface: the register
// list's delivery columns and variant lens (Y-82/Y-94). Siblings: CatalogNodeView
// .browser / .add-columns / .add-eras.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { RegisterShow } from "./api";
import { getRelatedDocuments, getShow, getStates } from "./api";
import CatalogNodeView from "./CatalogNodeView.svelte";
import {
  clickVariantChip,
  columnedRegisterNode,
  delivery,
  registerShow,
  splitColumnRegisterNode,
  withLisaVariants,
} from "./catalog-node-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { windowStore } from "./window.svelte";

// CatalogNodeView reads one node via `getShow(fqidPath)` and switches on
// `kind`. Mock that GET (mirrors ConceptGroupView's api-mock style); keep
// the rest of api.ts real (the type exports + path helpers `catalog.ts` uses).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getWarnings: vi.fn().mockResolvedValue([]),
    getShow: vi.fn(),
    getStates: vi.fn(),
    getRelatedDocuments: vi.fn(),
  };
});

// A concept group whose members SPLIT across variants: `kon` + `disp` are
// `individer-15plus`, `arbetsstalle` is `arbetsstallen`, and the ungrouped
// `foretag` is a variant of its own — so a lens can leave the group with two
// members, with one, or with none.
function splitVariantGroupRegisterNode(): RegisterShow {
  return registerShow({
    children: [
      {
        fqid: "scb/lisa/kon",
        name: "Kön",
        deliveries: [delivery("individer-15plus", "Kon", ["2018-01-01", null])],
      },
      {
        fqid: "scb/lisa/disp",
        name: "Disponibel inkomst",
        deliveries: [
          delivery("individer-15plus", "CDISP", ["1968-01-01", null]),
        ],
      },
      {
        fqid: "scb/lisa/arbetsstalle",
        name: "Arbetsställe",
        deliveries: [
          delivery("arbetsstallen", "ArbstNr", ["2005-01-01", null]),
        ],
      },
      {
        fqid: "scb/lisa/foretag",
        name: "Företag",
        deliveries: [delivery("foretag", "ForetagNr", ["2005-01-01", null])],
      },
    ],
    groups: [
      {
        fqid: "group/scb/lisa/inkomstbegrepp",
        key: "inkomstbegrepp",
        label: "Inkomstbegrepp",
        source: "token",
        tags: [],
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
  });
}

// `splitColumnRegisterNode`'s `kon` + `disp` FOLDED into one concept-group row
// whose `disp` member is a REPRESENTATION FAMILY (#819) — `disp`'s FQID
// survives under BOTH variants (each delivers it under CDISP or CDISP5), so a
// lens must narrow the group's members by `(fqid, delivery_column)` rather
// than by FQID alone, or the representation the lensed variant does not
// deliver stays in the group (Y-94).
function splitVariantRepresentationGroupRegisterNode(): RegisterShow {
  return {
    ...splitColumnRegisterNode(),
    groups: [
      {
        fqid: "group/scb/lisa/inkomstbegrepp",
        key: "inkomstbegrepp",
        label: "Inkomstbegrepp",
        source: "token",
        tags: [],
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
  };
}

// The same register with `kon` + `disp` FOLDED into one concept-group row (#303).
// The row stands in for its members, so the column filter has to reach through it.
function groupedColumnRegisterNode(): RegisterShow {
  return {
    ...columnedRegisterNode(1),
    groups: [
      {
        fqid: "group/scb/lisa/inkomstbegrepp",
        key: "inkomstbegrepp",
        label: "Inkomstbegrepp",
        source: "token",
        tags: [],
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
  };
}

beforeEach(() => {
  vi.mocked(getShow).mockReset();
  vi.mocked(getStates).mockReset();
  vi.mocked(getRelatedDocuments).mockReset();
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

describe("CatalogNodeView register arm", () => {
  it("names the delivery columns beside each variable, dated only where there are several (Y-82)", async () => {
    vi.mocked(getShow).mockResolvedValue(columnedRegisterNode(1));

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
    vi.mocked(getShow).mockResolvedValue(columnedRegisterNode(1));

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
    vi.mocked(getShow).mockResolvedValue(
      withLisaVariants(columnedRegisterNode(2)),
    );

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
    vi.mocked(getShow).mockResolvedValue(
      withLisaVariants(columnedRegisterNode(2)),
    );

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
    vi.mocked(getShow).mockResolvedValue(
      withLisaVariants(columnedRegisterNode(2)),
    );

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
    vi.mocked(getShow).mockResolvedValue(
      withLisaVariants(columnedRegisterNode(2)),
    );

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
    vi.mocked(getShow).mockResolvedValue(
      withLisaVariants(splitColumnRegisterNode()),
    );

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
    vi.mocked(getShow).mockResolvedValue(
      withLisaVariants(splitVariantGroupRegisterNode()),
    );

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
    vi.mocked(getShow).mockResolvedValue(
      withLisaVariants(splitVariantGroupRegisterNode()),
    );

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
    vi.mocked(getShow).mockResolvedValue(
      withLisaVariants(splitVariantRepresentationGroupRegisterNode()),
    );

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
    vi.mocked(getShow).mockResolvedValue(groupedColumnRegisterNode());

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
    vi.mocked(getShow).mockResolvedValue(columnedRegisterNode(1));

    await render(CatalogNodeView, {
      fqidPath: "scb/lisa",
      regMetaVersion: "test",
      steward: "global",
      windowMinYear: 1960,
      vintageYear: 2024,
    });

    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();
    expect(page.getByRole("group", { name: "Variant" }).elements()).toEqual([]);
  });
});
