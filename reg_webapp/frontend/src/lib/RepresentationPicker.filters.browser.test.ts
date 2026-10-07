import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { GroupAxisModel } from "./api";
import type { PickerRepresentation } from "./catalog";
import RepresentationPicker, {
  type PickerBand,
} from "./RepresentationPicker.svelte";
import {
  AXES,
  clickFilter,
  edge,
  graph,
  graphNode,
  graphState,
  multiAxisBand,
  PROPS,
  row,
  visibleColumns,
} from "./representation-picker-test-helpers";

// RepresentationPicker drives the concept-group column picker. #908 adds
// dimension-type marking (per-row axis markers) + per-dimension filter controls
// (facet axis / population / coding). Render the component directly with `bands` +
// `axes` props — no API mocks needed; the picker is purely presentational.

// Split from RepresentationPicker.browser.test.ts by contract surface: #908 dimension marking + filters.
// Siblings: RepresentationPicker.{graph,graph-fallback,graph-history,labels,filters,staging,row-identity}.browser.test.ts.

describe("RepresentationPicker graph mode (#904)", () => {
  it("keeps #908 dimension filters above graph mode", async () => {
    const band = multiAxisBand();
    await render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      graph: graph({
        nodes: [
          graphNode(band.key, {
            states: [
              graphState({ delivery_column_name: "DIN1" }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "DIN2",
              }),
              graphState({
                state_id: "3",
                period_scope: "intervals",
                representation_run_id: 3,
                delivery_column_name: "DIN3",
              }),
            ],
          }),
          graphNode("scb/iot/next", {
            states: [
              graphState({
                state_id: "4",
                period_scope: "intervals",
                representation_run_id: 4,
                delivery_column_name: "NEXT",
              }),
            ],
          }),
        ],
        edges: [edge(band.key, "scb/iot/next")],
        focus_id: null,
      }),
      ...PROPS,
    });

    await expect
      .element(page.getByRole("group", { name: /Filter columns/ }))
      .toBeVisible();
    expect(document.querySelector(".graph-picker")).not.toBeNull();
    expect(document.querySelector(".col-list")).toBeNull();
    await expect
      .element(page.getByText("Showing 3 of 3 columns"))
      .toBeVisible();

    clickFilter("Familj");
    await expect
      .element(page.getByText("Showing 1 of 3 columns"))
      .toBeVisible();
    expect(document.querySelector(".graph-picker")).not.toBeNull();
    const graphText =
      document.querySelector(".graph-picker")?.textContent ?? "";
    expect(graphText).toContain("DIN2");
    expect(graphText).not.toContain("DIN1");
    expect(graphText).not.toContain("DIN3");
  });

  it("renders graph mode when a declared #908 facet axis has only one value", async () => {
    const aFqid = "scb/iot/dispink";
    const bFqid = "scb/iot/next";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Disponibel inkomst",
          registerPrefix: "scb/iot",
          rows: [row({ column: "DIN1", valueSetLabel: "kr" })],
          facetsByColumn: {
            DIN1: [{ axis: "enhet", value: "ind", label: "Individ" }],
          },
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "Next",
          registerPrefix: "scb/iot",
          rows: [row({ column: "NEXT" })],
        } satisfies PickerBand,
      ],
      axes: [{ name: "enhet", label: "Enhet" }],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [graphState({ delivery_column_name: "DIN1" })],
          }),
          graphNode(bFqid, {
            states: [
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "NEXT",
                valid_from: "2011-01-01",
                valid_to: "9999-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(aFqid, bFqid)],
        focus_id: null,
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".graph-picker")) {
        throw new Error("graph picker not rendered");
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    expect(document.body.textContent).toContain("Enhet");
    expect(document.body.textContent).toContain("Individ");
  });
});

describe("RepresentationPicker dimension marking + filters (#908)", () => {
  it("renders a filter fieldset per discriminating dimension, naming its kind", async () => {
    await render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
    });
    // Both facet axes discriminate (enhet: ind/fam; hush: h1/h2) → two fieldsets.
    // Coding is constant ("kr") and variant constant ("v") → no control for those.
    await expect
      .element(page.getByRole("group", { name: /Filter columns/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("group", { name: "Enhet" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("group", { name: "Hushallsbegrepp" }))
      .toBeVisible();
    expect(page.getByRole("group", { name: "Coding" }).elements()).toEqual([]);
    expect(page.getByRole("group", { name: "Variant" }).elements()).toEqual([]);

    // Each row is marked with value-only facet pills; the axis rides in the
    // marker's accessible name, so the row reads "Enhet: Individ" to AT.
    await expect
      .element(page.getByRole("checkbox", { name: /Enhet: Individ/ }).first())
      .toBeVisible();
  });

  it("global select-all is checked when every column is, and partially checked after one is cleared", async () => {
    await render(RepresentationPicker, {
      bands: [
        multiAxisBand(),
        {
          key: "scb/iot/other",
          name: "Other income",
          registerPrefix: "scb/iot",
          rows: [row({ column: "DIN4", valueSetLabel: "kr" })],
        } satisfies PickerBand,
      ],
      axes: [],
      ...PROPS,
    });
    const selectAll = page.getByRole("checkbox", {
      name: "Select all columns",
      exact: true,
    });
    await expect.element(selectAll).not.toBeChecked();
    await expect.element(selectAll).not.toBePartiallyChecked();

    await selectAll.click();
    await expect.element(page.getByText("+4 columns")).toBeVisible();
    await expect.element(selectAll).toBeChecked();
    for (const column of ["DIN1", "DIN2", "DIN3", "DIN4"]) {
      await expect
        .element(page.getByRole("checkbox", { name: new RegExp(column) }))
        .toBeChecked();
    }

    await page.getByRole("checkbox", { name: /DIN1/ }).click();
    await expect.element(page.getByText("+3 columns")).toBeVisible();
    await expect.element(selectAll).toBePartiallyChecked();
  });

  it("selecting a facet value narrows the visible rows; clearing restores them", async () => {
    await render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
    });
    await expect
      .element(page.getByText("Showing 3 of 3 columns"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2", "DIN3"]);

    // Filter Hushallsbegrepp → "Familj" (h2): only DIN2 carries it.
    clickFilter("Familj");
    await expect
      .element(page.getByText("Showing 1 of 3 columns"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN2"]);

    // Clear → all rows back.
    await page.getByRole("button", { name: "Clear filters" }).click();
    await expect
      .element(page.getByText("Showing 3 of 3 columns"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2", "DIN3"]);
  });

  it("toggle-all acts on visible rows only: a hidden-but-selected row survives select-all then deselect-all", async () => {
    const onapply = vi.fn();
    await render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
      onapply,
    });
    // Select DIN3 (enhet=fam, hush=h1) via its row checkbox.
    const din3 = await vi.waitFor(() => {
      const cb = [
        ...document.querySelectorAll<HTMLInputElement>(
          ".col-list .row-btn input.cbox",
        ),
      ][2];
      if (!cb) {
        throw new Error("DIN3 row checkbox not yet rendered");
      }
      return cb;
    });
    din3.click();
    await expect.element(page.getByText("+1 column")).toBeVisible();

    // Filter Enhet → "Individ" (ind): DIN3 (fam) is now hidden but still selected.
    clickFilter("Individ");
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2"]);

    const selectAll = page.getByRole("checkbox", {
      name: "Select all columns",
    });
    // Select all → adds the 2 visible rows; the hidden DIN3 stays selected (3 total).
    await selectAll.click();
    await expect
      .element(page.getByText("+3 columns (1 hidden by filters)"))
      .toBeVisible();
    // Deselect all → clears the 2 visible rows only; the hidden DIN3 survives.
    await selectAll.click();
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2"]);

    // The surviving hidden selection still commits.
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    const committed = onapply.mock.calls[0][0].adds as {
      row: PickerRepresentation;
    }[];
    expect(committed.map((s) => s.row.column)).toEqual(["DIN3"]);
  });

  it("'No columns match' shows when a filter empties the list", async () => {
    await render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
    });
    await expect
      .element(page.getByText("Showing 3 of 3 columns"))
      .toBeVisible();
    // enhet=fam (DIN3) AND hush=h2 (DIN2) is an empty intersection.
    clickFilter("Konsumtionsenhet");
    clickFilter("Familj");
    await expect
      .element(page.getByText("No columns match the active filters."))
      .toBeVisible();
  });

  // C1: a whole-variable faceted member has a null delivery_column, so its facets
  // arrive band-level (the GROUP view sets `band.facets`), NOT keyed by column. The
  // common shape is a month-faceted group: one variable per month, each band carrying
  // its own `month`-axis facet on the whole variable. #908 must still surface the
  // facet filter + per-row markers for these.
  const MONTH_AXES: GroupAxisModel[] = [{ name: "month", label: "Month" }];
  function monthBand(slug: string, col: string, value: string, label: string) {
    return {
      key: `scb/x/${slug}`,
      name: label,
      registerPrefix: "scb/x",
      rows: [row({ column: col, valueSetLabel: "kr" })],
      // Band-level facets — no `facetsByColumn` (the whole-variable shape).
      facets: [{ axis: "month", value, label }],
    } satisfies PickerBand;
  }

  it("band-level facets (whole-variable members) render the facet filter + per-row markers (C1)", async () => {
    await render(RepresentationPicker, {
      bands: [
        monthBand("jan", "JAN", "01", "January"),
        monthBand("feb", "FEB", "02", "February"),
      ],
      axes: MONTH_AXES,
      ...PROPS,
    });
    // The month axis discriminates (01/02) → one filter fieldset named "Month".
    await expect
      .element(page.getByRole("group", { name: /Filter columns/ }))
      .toBeVisible();
    const legends = [...document.querySelectorAll(".dim-filter legend")].map(
      (l) => l.textContent?.trim(),
    );
    expect(legends).toEqual(["Month"]);
    // Each row shows its band-level facet as a per-row marker (the fallback path).
    const markers = [
      ...document.querySelectorAll(".col-row .facet-markers"),
    ].map((m) => m.textContent);
    expect(markers.join(" ")).toContain("January");
    expect(markers.join(" ")).toContain("February");

    // Filtering by the band-level facet narrows the list.
    expect(visibleColumns()).toEqual(["JAN", "FEB"]);
    clickFilter("February");
    await expect
      .element(page.getByText("Showing 1 of 2 columns"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["FEB"]);
  });

  it("suppresses repeated operational definitions and constant coding context when facets distinguish rows (#959)", async () => {
    const axes: GroupAxisModel[] = [{ name: "rank", label: "Rank" }];
    await render(RepresentationPicker, {
      bands: [
        {
          key: "scb/lisa/agi1faman",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens största förvärvskälla under året.",
          rows: [row({ column: "AGI1FAMAN", valueSetLabel: "Förekomst" })],
          facets: [{ axis: "rank", value: "1", label: "Största" }],
        },
        {
          key: "scb/lisa/agi2faman",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens näst största förvärvskälla under året.",
          rows: [row({ column: "AGI2FAMAN", valueSetLabel: "Förekomst" })],
          facets: [{ axis: "rank", value: "2", label: "Näst största" }],
        },
        {
          key: "scb/lisa/agi3faman",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens tredje största förvärvskälla under året.",
          rows: [row({ column: "AGI3FAMAN", valueSetLabel: "Förekomst" })],
          facets: [{ axis: "rank", value: "3", label: "Tredje största" }],
        },
      ],
      axes,
      ...PROPS,
    });

    await vi.waitFor(() => {
      const text = document.body.textContent ?? "";
      expect(text).toContain("Största");
      expect(text).toContain("Näst största");
      expect(text).toContain("Tredje största");
    });
    expect(document.body.textContent).not.toContain("Variabeln anger");
    expect(document.body.textContent).not.toContain("op def");
    expect(document.body.textContent).not.toContain("Förekomst");
  });

  it("keeps operational definitions when only a majority share an axis-carried stem (#959)", async () => {
    const axes: GroupAxisModel[] = [{ name: "rank", label: "Rank" }];
    await render(RepresentationPicker, {
      bands: [
        {
          key: "scb/lisa/first",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens första förvärvskälla under året.",
          rows: [row({ column: "FIRST" })],
          facets: [{ axis: "rank", value: "1", label: "Första" }],
        },
        {
          key: "scb/lisa/second",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens andra förvärvskälla under året.",
          rows: [row({ column: "SECOND" })],
          facets: [{ axis: "rank", value: "2", label: "Andra" }],
        },
        {
          key: "scb/lisa/manual",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Manually curated source classification for special cases.",
          rows: [row({ column: "MANUAL" })],
          facets: [{ axis: "rank", value: "x", label: "Special" }],
        },
      ],
      axes,
      ...PROPS,
    });

    await vi.waitFor(() => {
      const text = document.body.textContent ?? "";
      expect(text).toContain(
        "Variabeln anger familjens första förvärvskälla under året.",
      );
      expect(text).toContain(
        "Variabeln anger familjens andra förvärvskälla under året.",
      );
      expect(text).toContain(
        "Manually curated source classification for special cases.",
      );
    });
  });
});
