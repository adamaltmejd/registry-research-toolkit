import { describe, expect, it } from "vitest";
import type {
  PickerBandFacets,
  PickerDimension,
  PickerRepresentation,
} from "./catalog";
import {
  coverageFromStates,
  pickerFilterDimensions,
  pickerRowPasses,
  representationInWindow,
} from "./catalog";
import { state } from "./catalog-test-helpers";

// Split from catalog.test.ts by contract surface: picker filters + window.
// Siblings: catalog.{browse,picker,picker-filters,value-sets}.test.ts.

describe("pickerFilterDimensions / pickerRowPasses (#908)", () => {
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
  // A band carrying a single representation column with the given facets on that column.
  function fband(
    column: string,
    facets: { axis: string; value: string; label: string }[],
    rowOver: Partial<PickerRepresentation> = {},
  ) {
    return {
      rows: [row({ column, ...rowOver })],
      facetsByColumn: { [column]: facets },
    };
  }
  it("pickerRowPasses: AND across dimensions, OR within a dimension", () => {
    const dims: PickerDimension[] = [
      {
        kind: "facet",
        key: "hush",
        label: "Hushållsbegrepp",
        values: [
          { value: "h1", label: "A" },
          { value: "h2", label: "B" },
        ],
      },
      {
        kind: "variant",
        key: "variant",
        label: "Variant",
        values: [
          { value: "ind", label: "ind" },
          { value: "fam", label: "fam" },
        ],
      },
    ];
    const band = fband("DIN1", [{ axis: "hush", value: "h1", label: "A" }], {
      variant: "ind",
    });
    const theRow = band.rows[0];
    // No selection → passes.
    expect(pickerRowPasses(theRow, band, dims, {})).toBe(true);
    // Matching facet → passes.
    expect(pickerRowPasses(theRow, band, dims, { hush: new Set(["h1"]) })).toBe(
      true,
    );
    // Non-matching facet → fails.
    expect(pickerRowPasses(theRow, band, dims, { hush: new Set(["h2"]) })).toBe(
      false,
    );
    // OR within: either value selected passes.
    expect(
      pickerRowPasses(theRow, band, dims, { hush: new Set(["h1", "h2"]) }),
    ).toBe(true);
    // AND across: facet matches but variant doesn't → fails.
    expect(
      pickerRowPasses(theRow, band, dims, {
        hush: new Set(["h1"]),
        variant: new Set(["fam"]),
      }),
    ).toBe(false);
  });

  it("pickerRowPasses: a row lacking a facet on a SELECTED axis fails that axis", () => {
    const dims: PickerDimension[] = [
      {
        kind: "facet",
        key: "hush",
        label: "Hushållsbegrepp",
        values: [{ value: "h1", label: "A" }],
      },
    ];
    // The band carries no facet on `hush` for this column.
    const band = { rows: [row({ column: "C" })], facetsByColumn: {} };
    expect(
      pickerRowPasses(band.rows[0], band, dims, { hush: new Set(["h1"]) }),
    ).toBe(false);
  });

  it("pickerRowPasses: coding branch matches the row's value-set label; code-less always fails", () => {
    const dims: PickerDimension[] = [
      {
        kind: "coding",
        key: "coding",
        label: "Coding",
        values: [
          { value: "SNI 2002", label: "SNI 2002" },
          { value: "SNI 2007", label: "SNI 2007" },
        ],
      },
    ];
    const band = { rows: [row({ column: "C", valueSetLabel: "SNI 2002" })] };
    const coded = band.rows[0];
    // In the selected coding set → passes; not in it → fails.
    expect(
      pickerRowPasses(coded, band, dims, { coding: new Set(["SNI 2002"]) }),
    ).toBe(true);
    expect(
      pickerRowPasses(coded, band, dims, { coding: new Set(["SNI 2007"]) }),
    ).toBe(false);
    // A code-less row (valueSetLabel "") is NOT a coding choice — it fails ANY
    // active coding filter, even one whose set is non-empty (intended design).
    const bare = row({ column: "D", valueSetLabel: "" });
    expect(
      pickerRowPasses(bare, { rows: [bare] }, dims, {
        coding: new Set(["SNI 2002", "SNI 2007"]),
      }),
    ).toBe(false);
  });

  // ── C2: facet key namespacing vs. built-in dimension keys ──────────────────
  it("namespaces a facet key so an axis named 'coding' can't collide with the built-in coding dim (C2)", () => {
    // A declared axis literally named "coding", AND rows that also vary on the
    // built-in coding (value-set label). Both must surface as DISTINCT dimensions.
    const bands: PickerBandFacets[] = [
      {
        rows: [row({ column: "A", valueSetLabel: "SNI 2002" })],
        facetsByColumn: { A: [{ axis: "coding", value: "x", label: "X" }] },
      },
      {
        rows: [row({ column: "B", valueSetLabel: "SNI 2007" })],
        facetsByColumn: { B: [{ axis: "coding", value: "y", label: "Y" }] },
      },
    ];
    const dims = pickerFilterDimensions(bands, [
      { name: "coding", label: "Coding axis" },
    ]);
    // Two distinct dimensions: the facet (namespaced) and the built-in coding.
    const facetDim = dims.find((d) => d.kind === "facet");
    const codingDim = dims.find((d) => d.kind === "coding");
    expect(facetDim?.key).toBe("facet:coding");
    expect(facetDim?.axis).toBe("coding");
    expect(codingDim?.key).toBe("coding");
    expect(codingDim?.axis).toBeUndefined();
    // Distinct keys → no duplicate Svelte #each key, no shared selection slot.
    expect(new Set(dims.map((d) => d.key)).size).toBe(dims.length);
  });

  it("a selection on the facet axis 'coding' does not bleed into the built-in coding dim (C2)", () => {
    const bands: PickerBandFacets[] = [
      {
        rows: [row({ column: "A", valueSetLabel: "SNI 2002" })],
        facetsByColumn: { A: [{ axis: "coding", value: "x", label: "X" }] },
      },
      {
        rows: [row({ column: "B", valueSetLabel: "SNI 2007" })],
        facetsByColumn: { B: [{ axis: "coding", value: "y", label: "Y" }] },
      },
    ];
    const dims = pickerFilterDimensions(bands, [
      { name: "coding", label: "Coding axis" },
    ]);
    const bandA = bands[0];
    const rowA = bandA.rows[0]; // facet coding=x, value-set "SNI 2002"
    // Select the FACET value "x" only — the built-in coding dim has no selection, so
    // it imposes no constraint; rowA passes (its facet IS "x").
    expect(
      pickerRowPasses(rowA, bandA, dims, { "facet:coding": new Set(["x"]) }),
    ).toBe(true);
    // Select the built-in CODING value "SNI 2007" only — rowA's value-set is
    // "SNI 2002", so it fails. The facet selection slot ("facet:coding") is separate
    // and untouched, proving no bleed: the same literal "coding" lives in two slots.
    expect(
      pickerRowPasses(rowA, bandA, dims, { coding: new Set(["SNI 2007"]) }),
    ).toBe(false);
    // And selecting the facet "x" must NOT satisfy a built-in coding filter for a
    // different value-set: distinct slots, no cross-talk.
    expect(
      pickerRowPasses(rowA, bandA, dims, {
        "facet:coding": new Set(["x"]),
        coding: new Set(["SNI 2007"]),
      }),
    ).toBe(false);
  });
});

describe("pickerWindowYears + representationInWindow (#678 dimming)", () => {
  const row = (from: string, to: string) => ({ from, to });

  it("an open-ended row reaches past any finite window end", () => {
    expect(
      representationInWindow(row("2010-01-01", "9999-12-31"), [2030, 2040]),
    ).toBe(true);
  });
});

describe("coverageFromStates (#615 availability span)", () => {
  it("the open-ended sentinel leaves the END unbounded (null), start preserved", () => {
    // `9999-12-31` = "still delivered" → `to: null`; the picker projects the open
    // end to the slider's vintage ceiling, never a literal 9999 track.
    expect(
      coverageFromStates([
        state({
          state_id: "1",
          valid_from: "2005-01-01",
          valid_to: "9999-12-31",
        }),
      ]),
    ).toEqual({ from: 2005, to: null });
  });

  it("the yearless floor (0001) leaves the START unbounded but PRESERVES a finite end", () => {
    // The round-1 regression: `0001-01-01..2008-12-31` (unknown start, KNOWN end)
    // must keep `to: 2008` (only the start is unbounded), NOT collapse the whole
    // span to null — else a 2010–2015 selection loses its "Not delivered after
    // 2008" warning (Codex P2 round 2, Fix A).
    expect(
      coverageFromStates([
        state({
          state_id: "1",
          valid_from: "0001-01-01",
          valid_to: "2008-12-31",
        }),
      ]),
    ).toEqual({ from: null, to: 2008 });
  });

  it("a wholly-sentinel state (0001..9999) is unbounded on BOTH sides → null", () => {
    // Both bounds are sentinels, so coverage is fully unknown — no finite side to
    // draw or gap against (NOT { from: 1, … }, which would let the slider emit
    // out-of-grammar wires like `1..2026`).
    expect(
      coverageFromStates([
        state({
          state_id: "1",
          valid_from: "0001-01-01",
          valid_to: "9999-12-31",
        }),
      ]),
    ).toBeNull();
  });

  it("a 0001-floor state alongside a real-year state → finite start from the real year", () => {
    expect(
      coverageFromStates([
        state({
          state_id: "1",
          valid_from: "0001-01-01",
          valid_to: "2008-12-31",
        }),
        state({
          state_id: "2",
          valid_from: "2002-01-01",
          valid_to: "2010-12-31",
        }),
      ]),
    ).toEqual({ from: 2002, to: 2010 });
  });
});
