import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { GroupAxisModel } from "./api";
import type { PickerRepresentation } from "./catalog";
import RepresentationPicker, {
  type PickerBand,
} from "./RepresentationPicker.svelte";

// RepresentationPicker drives the concept-group column picker. #908 adds
// dimension-type marking (per-row axis markers) + per-dimension filter controls
// (facet axis / population / coding). Render the component directly with `bands` +
// `axes` props — no API mocks needed; the picker is purely presentational.

function row(over: Partial<PickerRepresentation>): PickerRepresentation {
  return {
    key: `${over.variant ?? "v"}::${over.column ?? "Col"}`,
    variant: "v",
    variantLabel: over.variant ?? "v",
    column: over.column ?? "Col",
    representation: over.column ?? "Col",
    from: "2000-01-01",
    to: "2010-12-31",
    windows: [{ from: "2000-01-01", to: "2010-12-31" }],
    period: "2000 – 2010",
    wirePeriod: "2000..2010",
    valueSetLabel: "",
    codingsVary: false,
    renamedColumns: [],
    ...over,
  };
}

/** A multi-axis representation group: one band, three delivery-column rows, each a
 * member with a distinct (enhet, hush) facet pair — the shape the #819 families
 * have and that #908's filters narrow. */
function multiAxisBand(): PickerBand {
  return {
    key: "scb/iot/dispink",
    name: "Disponibel inkomst",
    registerPrefix: "scb/iot",
    rows: [
      row({ column: "DIN1", valueSetLabel: "kr" }),
      row({ column: "DIN2", valueSetLabel: "kr" }),
      row({ column: "DIN3", valueSetLabel: "kr" }),
    ],
    facetsByColumn: {
      DIN1: [
        { axis: "enhet", value: "ind", label: "Individ" },
        { axis: "hush", value: "h1", label: "Hushall" },
      ],
      DIN2: [
        { axis: "enhet", value: "ind", label: "Individ" },
        { axis: "hush", value: "h2", label: "Familj" },
      ],
      DIN3: [
        { axis: "enhet", value: "fam", label: "Konsumtionsenhet" },
        { axis: "hush", value: "h1", label: "Hushall" },
      ],
    },
  };
}

const AXES: GroupAxisModel[] = [
  { name: "enhet", label: "Enhet" },
  { name: "hush", label: "Hushallsbegrepp" },
];

const PROPS = {
  window: null,
  canAdd: true,
  onapply: vi.fn(),
} as const;

/** The delivery-column chips of the currently-visible column ROWS (not the filter
 * fieldsets). */
function visibleColumns(): (string | undefined)[] {
  return [...document.querySelectorAll(".col-list .col-row .col-chip")].map(
    (c) => c.textContent?.replace("↗", "").trim(),
  );
}

/** Click a filter PILL (a labelled checkbox inside a `.dim-filter` fieldset) by its
 * value text — scoped to `.dim-filters` so it never hits a row checkbox. */
function clickFilter(value: string): void {
  const pill = [...document.querySelectorAll(".dim-filters .filter-pill")].find(
    (p) => p.textContent?.trim() === value,
  ) as HTMLElement | undefined;
  if (!pill) {
    throw new Error(`filter pill not found: ${value}`);
  }
  pill.click();
}

describe("RepresentationPicker dimension marking + filters (#908)", () => {
  it("renders a filter fieldset per discriminating dimension, naming its kind", async () => {
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
    });
    // Both facet axes discriminate (enhet: ind/fam; hush: h1/h2) → two fieldsets.
    // Coding is constant ("kr") and variant constant ("v") → no control for those.
    await expect
      .element(page.getByRole("group", { name: /Filter columns/ }))
      .toBeVisible();
    const legends = [...document.querySelectorAll(".dim-filter legend")].map(
      (l) => l.textContent?.trim(),
    );
    expect(legends).toEqual(["Enhet", "Hushallsbegrepp"]);
    // Each row is marked with its axis dimension markers (axis label + value).
    const markers = document.querySelector(".col-row .facet-markers");
    expect(markers?.textContent).toContain("Individ");
  });

  it("renders global select-all as an integrated row with selected and indeterminate states", async () => {
    render(RepresentationPicker, {
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
    const selectAllRow = await vi.waitFor(() => {
      const row = document.querySelector<HTMLLabelElement>(
        ".col-list > .select-all-row > label.select-all",
      );
      if (!row) {
        throw new Error("global select-all row not rendered");
      }
      return row;
    });
    expect(selectAllRow.classList.contains("integrated-list-row")).toBe(true);
    expect(selectAllRow.classList.contains("row-btn")).toBe(true);

    const selectAllBox =
      selectAllRow.querySelector<HTMLInputElement>("input.cbox");
    expect(selectAllBox).not.toBeNull();
    expect(selectAllBox?.checked).toBe(false);
    expect(selectAllBox?.indeterminate).toBe(false);

    // Click the row label, not the checkbox itself: the full integrated row toggles.
    selectAllRow.click();
    await expect.element(page.getByText("+4 columns")).toBeVisible();
    expect(selectAllBox?.checked).toBe(true);
    expect(selectAllBox?.indeterminate).toBe(false);
    expect(selectAllRow.classList.contains("selected")).toBe(true);
    const rowBoxes = [
      ...document.querySelectorAll<HTMLInputElement>(
        ".col-list .col-row .row-btn input.cbox",
      ),
    ];
    expect(rowBoxes).toHaveLength(4);
    expect(rowBoxes.every((box) => box.checked)).toBe(true);

    rowBoxes[0].click();
    await expect.element(page.getByText("+3 columns")).toBeVisible();
    expect(selectAllBox?.checked).toBe(false);
    expect(selectAllBox?.indeterminate).toBe(true);
    expect(selectAllRow.classList.contains("selected")).toBe(false);
  });

  it("a single-value group surfaces NO filter controls", async () => {
    render(RepresentationPicker, {
      bands: [
        {
          key: "scb/x/y",
          name: "Y",
          registerPrefix: "scb/x",
          rows: [row({ column: "Cee", valueSetLabel: "one" })],
        } satisfies PickerBand,
      ],
      axes: [],
      ...PROPS,
    });
    await vi.waitFor(() => {
      if (!document.querySelector(".col-row .col-chip")) {
        throw new Error("row not rendered yet");
      }
    });
    expect(visibleColumns()).toEqual(["Cee"]);
    expect(document.querySelector(".dim-filters")).toBeNull();
  });

  it("selecting a facet value narrows the visible rows; clearing restores them", async () => {
    render(RepresentationPicker, {
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

  it("filtering is presentation-only: a hidden selected column still commits, flagged in the footer", async () => {
    const onapply = vi.fn();
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
      onapply,
    });
    // Select DIN3 (carries enhet=fam, hush=h1) via its row checkbox.
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

    // Now filter Enhet → "Individ" (ind): DIN3 (fam) is hidden.
    clickFilter("Individ");
    // The selection persists and the footer signals the hidden selection.
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2"]);

    // Committing still includes the hidden-but-selected DIN3.
    await page.getByRole("button", { name: "Apply staged changes" }).click();
    expect(onapply).toHaveBeenCalledTimes(1);
    const committed = onapply.mock.calls[0][0].adds as {
      row: PickerRepresentation;
    }[];
    expect(committed.map((s) => s.row.column)).toEqual(["DIN3"]);
  });

  it("toggle-all acts on visible rows only: a hidden-but-selected row survives select-all then deselect-all", async () => {
    const onapply = vi.fn();
    render(RepresentationPicker, {
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
    await page.getByRole("button", { name: "Apply staged changes" }).click();
    expect(onapply).toHaveBeenCalledTimes(1);
    const committed = onapply.mock.calls[0][0].adds as {
      row: PickerRepresentation;
    }[];
    expect(committed.map((s) => s.row.column)).toEqual(["DIN3"]);
  });

  it("'No columns match' shows when a filter empties the list", async () => {
    render(RepresentationPicker, {
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
    render(RepresentationPicker, {
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
});

describe("codingsVaryNudge deep link (#905)", () => {
  /** Render one band whose single coding-varying row carries the nudge, then return
   * the nudge anchor's `href`. */
  async function nudgeHref(band: PickerBand): Promise<string> {
    render(RepresentationPicker, { bands: [band], axes: [], ...PROPS });
    const nudge = await vi.waitFor(() => {
      const a = document.querySelector(".rep-picker .codings-vary");
      if (!a) {
        throw new Error("codings-vary nudge not yet rendered");
      }
      return a;
    });
    expect(nudge.tagName).toBe("A");
    return nudge.getAttribute("href") ?? "";
  }

  // The leaf branch (band.href undefined → current path) is covered in
  // BindingLeafView.browser.test.ts. This covers the GROUP branch: when a band
  // carries `href` (the member's own leaf), the nudge links to that leaf with the
  // `codes` param carrying the ROW's `(variant, column)` identity — `variant::column`
  // (#905), merged into the href's existing query.
  it("a coding-varying row links to the member leaf with a (variant, column) ?codes deep link", async () => {
    const href = await nudgeHref({
      key: "scb/lisa/yrkesreg",
      name: "Yrkesregistret",
      registerPrefix: "scb/lisa",
      href: "/catalog/scb/lisa/yrkesreg",
      rows: [row({ variant: "individer", column: "Yrke", codingsVary: true })],
    } satisfies PickerBand);
    // The member leaf + `codes=<variant>::<column>` + states hash — clean query, the
    // row's variant carried so a shared column isolates THIS variant's coding.
    expect(href).toBe(
      "/catalog/scb/lisa/yrkesreg?codes=individer%3A%3AYrke#states-heading",
    );
  });

  // The nudge means "this column's coding changed OVER TIME — see the value sets",
  // which is inherently a FULL-HISTORY inspection (#905, Codex P2). So when `band.href`
  // carries an active `?period`, the nudge DROPS it (taking only the member path) and
  // emits a CLEAN `?codes=…` query — a period-narrowed leaf, focused via `?codes`, must
  // show the column's full coding history, not the period-scoped subset.
  it("drops an inherited ?period and emits a clean ?codes-only deep link", async () => {
    const href = await nudgeHref({
      key: "scb/lisa/yrkesreg",
      name: "Yrkesregistret",
      registerPrefix: "scb/lisa",
      href: "/catalog/scb/lisa/yrkesreg?period=2020",
      rows: [row({ variant: "v", column: "Yrke", codingsVary: true })],
    } satisfies PickerBand);
    // No `period` survives; the only param is the (variant, column) composite.
    expect(href).toBe(
      "/catalog/scb/lisa/yrkesreg?codes=v%3A%3AYrke#states-heading",
    );
    expect(href).not.toContain("period");
    // Exactly ONE `?` (no double query separator).
    expect(href.match(/\?/g)).toHaveLength(1);
  });
});

describe("RepresentationPicker sequential-rename hint (#902)", () => {
  // The picker collapses a variable's sequential column RENAME (non-overlapping eras,
  // distinct names) into ONE row led by the latest column, surfacing the earlier name(s)
  // as a quiet inline ".rename-hint" ("was OldCol"). This is normally produced by
  // pickerRepresentations; here we feed the collapsed `renamedColumns` directly to test
  // the render path (the `{@render renameHint(...)}` snippet) in isolation.
  it("renders a 'was <old>' hint for a collapsed rename, and none when empty", async () => {
    render(RepresentationPicker, {
      bands: [
        {
          key: "scb/x/renamed",
          name: "Renamed",
          registerPrefix: "scb/x",
          rows: [row({ column: "NewCol", renamedColumns: ["OldCol"] })],
        } satisfies PickerBand,
        {
          key: "scb/x/plain",
          name: "Plain",
          registerPrefix: "scb/x",
          rows: [row({ column: "PlainCol", renamedColumns: [] })],
        } satisfies PickerBand,
      ],
      axes: [],
      ...PROPS,
    });
    await vi.waitFor(() => {
      if (document.querySelectorAll(".col-row .col-chip").length < 2) {
        throw new Error("rows not rendered yet");
      }
    });

    // The renamed band shows exactly one ".rename-hint" naming the earlier column.
    const hints = [...document.querySelectorAll(".rename-hint")];
    expect(hints).toHaveLength(1);
    expect(hints[0].textContent?.trim()).toBe("was OldCol");

    // The plain band's row carries NO ".rename-hint".
    const plainRow = [...document.querySelectorAll(".col-row")].find((r) =>
      r.textContent?.includes("PlainCol"),
    );
    expect(plainRow?.querySelector(".rename-hint")).toBeNull();
  });
});
