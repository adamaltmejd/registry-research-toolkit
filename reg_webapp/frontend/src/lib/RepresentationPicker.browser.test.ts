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
    from: "2000-01-01",
    to: "2010-12-31",
    windows: [{ from: "2000-01-01", to: "2010-12-31" }],
    period: "2000 – 2010",
    wirePeriod: "2000..2010",
    valueSetLabel: "",
    codingsVary: false,
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
  onadd: vi.fn(),
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
    const onadd = vi.fn();
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
      onadd,
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
    await expect.element(page.getByText("1 column selected")).toBeVisible();

    // Now filter Enhet → "Individ" (ind): DIN3 (fam) is hidden.
    clickFilter("Individ");
    // The selection persists and the footer signals the hidden selection.
    await expect
      .element(page.getByText("1 column selected (1 hidden by filters)"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2"]);

    // Committing still includes the hidden-but-selected DIN3.
    await page.getByRole("button", { name: "Add to project" }).click();
    expect(onadd).toHaveBeenCalledTimes(1);
    const committed = onadd.mock.calls[0][0] as { row: PickerRepresentation }[];
    expect(committed.map((s) => s.row.column)).toEqual(["DIN3"]);
  });

  it("toggle-all acts on visible rows only: a hidden-but-selected row survives select-all then deselect-all", async () => {
    const onadd = vi.fn();
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
      onadd,
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
    await expect.element(page.getByText("1 column selected")).toBeVisible();

    // Filter Enhet → "Individ" (ind): DIN3 (fam) is now hidden but still selected.
    clickFilter("Individ");
    await expect
      .element(page.getByText("1 column selected (1 hidden by filters)"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2"]);

    const selectAll = page.getByRole("checkbox", {
      name: "Select all columns",
    });
    // Select all → adds the 2 visible rows; the hidden DIN3 stays selected (3 total).
    await selectAll.click();
    await expect
      .element(page.getByText("3 columns selected (1 hidden by filters)"))
      .toBeVisible();
    // Deselect all → clears the 2 visible rows only; the hidden DIN3 survives.
    await selectAll.click();
    await expect
      .element(page.getByText("1 column selected (1 hidden by filters)"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2"]);

    // The surviving hidden selection still commits.
    await page.getByRole("button", { name: "Add to project" }).click();
    expect(onadd).toHaveBeenCalledTimes(1);
    const committed = onadd.mock.calls[0][0] as { row: PickerRepresentation }[];
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
});
