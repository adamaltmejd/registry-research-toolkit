import { describe, expect, it } from "vitest";
import type { GraphState } from "./api";
import type { PickerRepresentation } from "./catalog";
import {
  addWindowBounds,
  coexistingColumns,
  deliveryColumnNamesFromStates,
  narrowStatesByModifier,
  pickerRepresentations,
  rowAddPeriod,
} from "./catalog";
import { state } from "./catalog-test-helpers";
import { VALUE_SET_VERSION_NONE } from "./period";

// Split from catalog.test.ts by contract surface: picker derivations.
// Siblings: catalog.{browse,picker,picker-filters,value-sets}.test.ts.

describe("narrowStatesByModifier (#678: picker honors the active narrowing)", () => {
  // A variable with two variants and two value-set versions — the picker should
  // offer only the rows consistent with whichever modifier is active.
  const states = [
    state({
      variant: "lastbilar",
      delivery_column_name: "SNI2002",
      value_set_version_label: "SNI 2002",
    }),
    state({
      variant: "bussar",
      delivery_column_name: "SNI2002",
      value_set_version_label: "SNI 2002",
    }),
    state({
      variant: "lastbilar",
      delivery_column_name: "SNI2007",
      value_set_version_label: "SNI 2007",
    }),
    state({
      variant: "personbilar",
      delivery_column_name: "SNI2002",
      value_set_version_label: "",
    }),
  ];

  it("the _none version sentinel matches the empty/default label", () => {
    const narrowed = narrowStatesByModifier(
      states,
      null,
      VALUE_SET_VERSION_NONE,
    );
    expect(narrowed.map((s) => s.variant)).toEqual(["personbilar"]);
  });
});

describe("pickerRepresentations (#678 direct picker)", () => {
  it("a sub-annual multi-month span emits an exact ISO range (not year-rounded)", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2020-02-01",
        valid_to: "2020-06-30",
      }),
    ]);
    // No single token covers Feb–Jun; the explicit range preserves the exact span,
    // and the year-aligned collapse does NOT apply (sub-annual endpoints).
    expect(row.wirePeriod).toBe("2020-02-01..2020-06-30");
  });

  // #678 finding 3: a column delivered in DISJOINT windows commits the comma-union
  // (the interrupted-series wire), never one continuous range over the gap years.
  it("a DISJOINT-delivery column emits a comma-list wire (gap years excluded)", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2005-01-01",
        valid_to: "2010-12-31",
      }),
      // A real 2011–2014 gap, then a second era.
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2015-01-01",
        valid_to: "2020-12-31",
      }),
    ]);
    expect(row.windows).toEqual([
      { from: "2005-01-01", to: "2010-12-31" },
      { from: "2015-01-01", to: "2020-12-31" },
    ]);
    expect(row.wirePeriod).toBe("2005..2010,2015..2020");
    // The outer span still spans both eras (the display "from..to").
    expect(row.from).toBe("2005-01-01");
    expect(row.to).toBe("2020-12-31");
  });

  it("fuses ADJACENT annual states into ONE window (no spurious comma split)", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
      }),
      // Back-to-back: 2019-01-01 is the day after 2018-12-31 → one continuous window.
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
    ]);
    expect(row.windows).toEqual([{ from: "2018-01-01", to: "2019-12-31" }]);
    expect(row.wirePeriod).toBe("2018..2019");
  });

  // #678 inc 2: the widened param accepts the group graph's `GraphState[]` too —
  // same `(variant, delivery_column)` enumeration, but its bounds are nullable.
  it("accepts graph states and normalizes a null end to an open-ended span", () => {
    // A minimal GraphState — null `valid_to` = still delivered. The function must
    // map it to the open-ended `9999-12-31` sentinel so the span renders "since
    // 2010" and the wire period stays unset (no in-grammar token for the end).
    const gstate = (over: Partial<GraphState>): GraphState =>
      ({
        state_id: "1",
        period_scope: "intervals",
        representation_run_id: 1,
        variant: "individer",
        variant_label: null,
        delivery_column_name: null,
        value_set_version_label: "1-siffrig",
        value_set_id: null,
        valid_from: null,
        valid_to: null,
        classification_slugs: [],
        ...over,
      }) as GraphState;

    const [row] = pickerRepresentations([
      gstate({
        delivery_column_name: "Kon",
        valid_from: "2010-01-01",
        valid_to: null, // unbounded end → open-ended
      }),
    ]);
    expect(row.key).toBe("individer::Kon");
    expect(row.column).toBe("Kon");
    expect(row.from).toBe("2010-01-01");
    expect(row.to).toBe("9999-12-31");
    expect(row.period).toBe("since 2010");
    expect(row.wirePeriod).toBeNull();
    expect(row.valueSetLabel).toBe("1-siffrig");
  });

  it("normalizes a null graph-state start to the yearless floor (until <year>)", () => {
    const [row] = pickerRepresentations([
      {
        state_id: "2",
        period_scope: "intervals",
        representation_run_id: 1,
        variant: "v1",
        variant_label: null,
        delivery_column_name: "Col",
        value_set_version_label: "",
        value_set_id: null,
        valid_from: null, // unknown start
        valid_to: "2008-12-31",
        classification_slugs: [],
        variant_family: null,
        variant_family_label: null,
      } as GraphState,
    ]);
    expect(row.from).toBe("0001-01-01");
    expect(row.to).toBe("2008-12-31");
    // The one-sided "until <year>" form, never the leaked sentinel year.
    expect(row.period).toBe("until 2008");
  });

  it("does NOT flag codingsVary when one value_set_id has inconsistent LABELS (the SUN case)", () => {
    // The same id 249 is labelled inconsistently across years/populations ('old' /
    // 'SUN 2020 NivaOld'). Keyed on the reliable id, this is ONE coding → no nudge.
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Sun",
        value_set_id: "249",
        value_set_version_label: "SUN 2020 NivaOld",
        valid_from: "2020-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        variant: "v1",
        delivery_column_name: "Sun",
        value_set_id: "249",
        value_set_version_label: "SUN 2000 NivaOld",
        valid_from: "2021-01-01",
        valid_to: "2021-12-31",
      }),
    ]);
    expect(row.codingsVary).toBe(false);
  });

  it("flags codingsVary on a null↔id transition (code-less → coded)", () => {
    // A null value_set_id is its own distinct value, so gaining (or losing) a coding
    // counts as a change.
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        value_set_id: null,
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
      }),
      state({
        variant: "v1",
        delivery_column_name: "Col",
        value_set_id: "42",
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
    ]);
    expect(row.codingsVary).toBe(true);
  });

  it("folds the renames but keeps a co-existing parallel PAIR separate", () => {
    // A MIX: A (2008–2010) → B (2011–2014) are a sequential rename (non-overlapping,
    // overlapping nothing else); X and Y both deliver 2015–2020 → a parallel pair. The
    // renames collapse to ONE row (led by B); X and Y each stay their own row (only
    // columns that overlap NOTHING fold — a column overlapping a sibling is parallel).
    const rows = pickerRepresentations([
      state({
        variant: "v",
        delivery_column_name: "A",
        valid_from: "2008-01-01",
        valid_to: "2010-12-31",
      }),
      state({
        variant: "v",
        delivery_column_name: "B",
        valid_from: "2011-01-01",
        valid_to: "2014-12-31",
      }),
      state({
        variant: "v",
        delivery_column_name: "X",
        valid_from: "2015-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        variant: "v",
        delivery_column_name: "Y",
        valid_from: "2015-01-01",
        valid_to: "2020-12-31",
      }),
    ]);
    const byCol = new Map(rows.map((r) => [r.column, r]));
    expect([...byCol.keys()].sort()).toEqual(["B", "X", "Y"]);
    expect(byCol.get("B")?.renamedColumns).toEqual(["A"]);
    expect(byCol.get("X")?.renamedColumns).toEqual([]);
    expect(byCol.get("Y")?.renamedColumns).toEqual([]);
    // The folded rename (B) commits null; the parallel pair (X, Y) commit their own
    // columns (#902).
    expect(byCol.get("B")?.representation).toBeNull();
    expect(byCol.get("X")?.representation).toBe("X");
    expect(byCol.get("Y")?.representation).toBe("Y");
  });

  it("does NOT fold a rename ACROSS variants (rename is one variable+population)", () => {
    // Same column-name lineage but different populations → each variant keeps its own
    // single-column row (per-variant scope), never folded together.
    const rows = pickerRepresentations([
      state({
        variant: "individer",
        delivery_column_name: "Old",
        valid_from: "2010-01-01",
        valid_to: "2014-12-31",
      }),
      state({
        variant: "familj",
        delivery_column_name: "New",
        valid_from: "2015-01-01",
        valid_to: "2020-12-31",
      }),
    ]);
    expect(rows.map((r) => r.key).sort()).toEqual([
      "familj::New",
      "individer::Old",
    ]);
    expect(rows.every((r) => r.renamedColumns.length === 0)).toBe(true);
  });
});

describe("coexistingColumns (#902 shared overlap leaf)", () => {
  it("returns columns whose windows overlap; excludes a sequential rename", () => {
    const set = coexistingColumns([
      {
        delivery_column_name: "A",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      },
      {
        delivery_column_name: "B",
        valid_from: "2012-01-01",
        valid_to: "2018-12-31",
      },
      {
        delivery_column_name: "C",
        valid_from: "2021-01-01",
        valid_to: "2025-12-31",
      },
    ]);
    // A and B overlap; C is wholly after both → a rename, not coexisting.
    expect([...set].sort()).toEqual(["A", "B"]);
  });

  it("treats a null (unbounded) end as overlapping everything after it", () => {
    const set = coexistingColumns([
      { delivery_column_name: "A", valid_from: "2010-01-01", valid_to: null },
      {
        delivery_column_name: "B",
        valid_from: "2030-01-01",
        valid_to: "2031-12-31",
      },
    ]);
    expect([...set].sort()).toEqual(["A", "B"]);
  });

  it("treats a null (unbounded) start as overlapping everything before it", () => {
    // A's null valid_from normalizes to YEARLESS_VALID_FROM (0001), so its window
    // reaches back before B and the two overlap.
    const set = coexistingColumns([
      {
        delivery_column_name: "A",
        valid_from: null,
        valid_to: "2005-12-31",
      },
      {
        delivery_column_name: "B",
        valid_from: "1990-01-01",
        valid_to: "1995-12-31",
      },
    ]);
    expect([...set].sort()).toEqual(["A", "B"]);
  });

  it("treats a fully-null-bounds column as overlapping everything", () => {
    // Both bounds null → window is the full 0001..9999 sentinel span, so it
    // overlaps any other column regardless of era.
    const set = coexistingColumns([
      { delivery_column_name: "A", valid_from: null, valid_to: null },
      {
        delivery_column_name: "B",
        valid_from: "2050-01-01",
        valid_to: "2055-12-31",
      },
    ]);
    expect([...set].sort()).toEqual(["A", "B"]);
  });

  it("treats columns touching at a single boundary instant as co-existing", () => {
    // A ends and B starts on the same day. The inclusive `<=` overlap counts this
    // as co-existing. This documents the explicit design choice so a future `<`
    // "cleanup" can't silently flip it.
    const set = coexistingColumns([
      {
        delivery_column_name: "A",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      },
      {
        delivery_column_name: "B",
        valid_from: "2015-12-31",
        valid_to: "2020-12-31",
      },
    ]);
    expect([...set].sort()).toEqual(["A", "B"]);
  });
});

describe("deliveryColumnNamesFromStates (the cart's unpinned column name)", () => {
  it("leads a rename within the period with the CURRENT column, superseded ones after", () => {
    // DINF → DINF83 → DINF86 over non-overlapping eras is ONE column renamed, the
    // same fold the picker's own rows present.
    expect(
      deliveryColumnNamesFromStates([
        state({
          delivery_column_name: "DINF83",
          valid_from: "1984-01-01",
          valid_to: "1985-12-31",
        }),
        state({
          delivery_column_name: "DINF",
          valid_from: "1981-01-01",
          valid_to: "1983-12-31",
        }),
        state({
          delivery_column_name: "DINF86",
          valid_from: "1990-01-01",
          valid_to: "9999-12-31",
        }),
      ]),
    ).toEqual(["DINF86", "DINF83", "DINF"]);
  });

  it("names nothing when co-existing columns leave a choice the file must pin", () => {
    // Two columns valid at the same instant: the cart may not pick one of them for
    // the researcher, so the row falls back to its FQID.
    expect(
      deliveryColumnNamesFromStates([
        state({
          delivery_column_name: "Ssyk3",
          valid_from: "2010-01-01",
          valid_to: "2020-12-31",
        }),
        state({
          delivery_column_name: "Ssyk4",
          valid_from: "2012-01-01",
          valid_to: "2020-12-31",
        }),
      ]),
    ).toEqual([]);
  });
});

describe("rowAddPeriod (#678 finding 3: honor the active period on add)", () => {
  // A picker row with explicit ISO bounds + its own full-span wire period. `windows`
  // defaults to ONE continuous window spanning from..to (the common case); disjoint
  // tests override it.
  const row = (
    over: Partial<PickerRepresentation> = {},
  ): PickerRepresentation => {
    const from = over.from ?? "2010-01-01";
    const to = over.to ?? "2020-12-31";
    return {
      key: "v1::Col",
      variant: "v1",
      variantLabel: "v1",
      column: "Col",
      representation: "Col",
      from,
      to,
      windows: [{ from, to }],
      period: "2010 – 2020",
      wirePeriod: "2010..2020",
      valueSetLabel: "",
      codingsVary: false,
      renamedColumns: [],
      ...over,
    };
  };
  // A year window expressed as inclusive ISO bounds (what `addWindowBounds` produces
  // from a year-grain window).
  const yr = (lo: number, hi: number) => ({
    from: `${lo}-01-01`,
    to: `${hi}-12-31`,
  });

  // #678 finding 1: a SUB-ANNUAL `?period` must commit at its real grain, NOT the
  // collapsed outer year. `addWindowBounds` produces the exact ISO bounds of the
  // selected quarter/term/month, so `rowAddPeriod` honors it.
  it("honors a sub-annual window at its true grain (a quarter stays a quarter, not its year)", () => {
    // The user picked 2020-Q1; the open-ended row clamps to exactly that quarter.
    const open = row({
      from: "2010-01-01",
      to: "9999-12-31",
      wirePeriod: null,
    });
    expect(rowAddPeriod(open, { from: "2020-01-01", to: "2020-03-31" })).toBe(
      "2020-Q1",
    );
  });

  it("honors a sub-annual month window (a long row clamped to a single month)", () => {
    expect(rowAddPeriod(row(), { from: "2015-03-01", to: "2015-03-31" })).toBe(
      "2015-03",
    );
  });

  it("clamps each disjoint window into the active window, dropping a window that falls outside", () => {
    const disjoint = row({
      from: "2005-01-01",
      to: "2020-12-31",
      windows: [
        { from: "2005-01-01", to: "2010-12-31" },
        { from: "2015-01-01", to: "2020-12-31" },
      ],
      wirePeriod: "2005..2010,2015..2020",
    });
    // A window over 2008–2017 keeps both eras but clamps each to the window edges:
    // 2008..2010 + 2015..2017.
    expect(rowAddPeriod(disjoint, yr(2008, 2017))).toBe(
      "2008..2010,2015..2017",
    );
    // A window inside the GAP keeps neither era → nothing to commit.
    expect(rowAddPeriod(disjoint, yr(2012, 2013))).toBeNull();
    // A window over only the first era keeps just it.
    expect(rowAddPeriod(disjoint, yr(2006, 2009))).toBe("2006..2009");
  });
});

describe("addWindowBounds (#678 finding 1: sub-annual period honored on add)", () => {
  it("a ?period that parses to no bound (_default) falls back to the year window", () => {
    expect(addWindowBounds("_default", [2000, 2004])).toEqual({
      from: "2000-01-01",
      to: "2004-12-31",
    });
  });
});

describe("year-independent delivery", () => {
  it("does not infer calendar co-delivery between independent and dated states", () => {
    expect(
      coexistingColumns([
        state({
          period_scope: "year_independent",
          valid_from: null,
          valid_to: null,
          delivery_column_name: "Independent",
        }),
        state({
          valid_from: "2020-01-01",
          valid_to: "2020-12-31",
          delivery_column_name: "Dated",
        }),
      ]),
    ).toEqual(new Set());
  });
});
