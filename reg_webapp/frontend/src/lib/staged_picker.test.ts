import { afterEach, describe, expect, it, vi } from "vitest";
import { getCatalogNode, type VariableStateModel } from "./api";
import {
  deliveryColumnRows,
  type PickerRepresentation,
  pickerRepresentations,
} from "./catalog";
import type { ProjectData } from "./project_data";
import { projectStore } from "./project_store.svelte";
import {
  applyStagedPicks,
  committedPickerRows,
  finalAddPeriodWires,
  finalSourcePeriodsForStagedAdds,
  nullBindingCommittedRowKeys,
  periodChangesWithStagedAdds,
  pickerRowKey,
  rowAddSegments,
  type StagedPick,
  type StagedPickerBand,
  stagedAddCandidates,
  stagedRemoveForCommitted,
} from "./staged_picker";

// Only the ONE call the staging stack makes on its own — `resolveBindingAt`'s
// `?period` resolve, one GET per staged add. Everything else in ./api stays real.
vi.mock("./api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./api")>()),
  getCatalogNode: vi.fn(),
}));

/** A folded LISA `individer` family row: one displayed row standing for two concrete
 * `register_variant`s (`individer-16plus` predecessor era + `individer-15plus`
 * successor era) delivering the same column over non-overlapping windows (#376). */
function foldedFamilyRow(): PickerRepresentation {
  return row({
    key: "individer-15plus{individer-16plus,individer-15plus}::Kon",
    variant: "individer-15plus",
    variantLabel: "Individer, 15 år och äldre",
    variantFamily: "individer-15plus",
    variantFamilyLabel: "Individer",
    column: "Kon",
    representation: "Kon",
    renamedColumns: [],
    from: "1990-01-01",
    to: "2023-12-31",
    windows: [
      { from: "1990-01-01", to: "2009-12-31" },
      { from: "2010-01-01", to: "2023-12-31" },
    ],
    variantSegments: [
      {
        variant: "individer-16plus",
        variantLabel: "Individer, 16 år och äldre",
        windows: [{ from: "1990-01-01", to: "2009-12-31" }],
      },
      {
        variant: "individer-15plus",
        variantLabel: "Individer, 15 år och äldre",
        windows: [{ from: "2010-01-01", to: "2023-12-31" }],
      },
    ],
    period: "1990 – 2023",
    wirePeriod: "1990..2009,2010..2023",
  });
}

function row(over: Partial<PickerRepresentation> = {}): PickerRepresentation {
  return {
    key: "ind::DINF86",
    variant: "ind",
    variantLabel: "ind",
    column: "DINF86",
    representation: null,
    from: "1981-01-01",
    to: "1995-12-31",
    windows: [{ from: "1981-01-01", to: "1995-12-31" }],
    period: "1981 - 1995",
    wirePeriod: "1981..1995",
    valueSetLabel: "",
    codingsVary: false,
    renamedColumns: ["DINF", "DINF83"],
    ...over,
  };
}

function band(rows: PickerRepresentation[]): StagedPickerBand {
  return {
    key: "scb/lisa/dinf",
    registerPrefix: "scb/lisa",
    rows,
  };
}

describe("committedPickerRows", () => {
  it("matches folded rename rows when a project pins a retired delivery column", () => {
    const r = row();
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA",
          register_variant: "scb/lisa/ind",
          period: { from: 1981, to: 1985 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "numeric",
              representation: "DINF83",
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.get(pickerRowKey(b, r))).toEqual(
      expect.objectContaining({
        representation: "DINF83",
        variable: "scb/lisa/dinf",
      }),
    );
  });

  it("keys family rows by variant family and removes every concrete source", () => {
    const r = row({
      key: "individer-15plus::Kon",
      variant: "individer-15plus",
      variantLabel: "Individer, 15 år och äldre",
      variantFamily: "individer-15plus",
      variantFamilyLabel: "Individer",
      column: "Kon",
      representation: "Kon",
      renamedColumns: [],
      windows: [
        { from: "1990-01-01", to: "2009-12-31" },
        { from: "2010-01-01", to: "2023-12-31" },
      ],
      variantSegments: [
        {
          variant: "individer-16plus",
          variantLabel: "Individer, 16 år och äldre",
          windows: [{ from: "1990-01-01", to: "2009-12-31" }],
        },
        {
          variant: "individer-15plus",
          variantLabel: "Individer, 15 år och äldre",
          windows: [{ from: "2010-01-01", to: "2023-12-31" }],
        },
      ],
    });
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA 1990-2009",
          register_variant: "scb/lisa/individer-16plus",
          period: { from: 1990, to: 2009 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "integer",
              representation: "Kon",
            },
          ],
        },
        {
          name: "LISA 2010-2023",
          register_variant: "scb/lisa/individer-15plus",
          period: { from: 2010, to: 2023 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "integer",
              representation: "Kon",
            },
          ],
        },
      ],
    };

    expect(pickerRowKey(b, r)).toBe(
      "scb/lisa/individer-15plus::scb/lisa/dinf::Kon",
    );
    const committed = committedPickerRows(draft, [b]).get(pickerRowKey(b, r));
    expect(committed?.removals).toEqual([
      {
        registerVariant: "scb/lisa/individer-16plus",
        variable: "scb/lisa/dinf",
        representation: "Kon",
      },
      {
        registerVariant: "scb/lisa/individer-15plus",
        variable: "scb/lisa/dinf",
        representation: "Kon",
      },
    ]);
    expect(
      stagedRemoveForCommitted(committed as NonNullable<typeof committed>),
    ).toHaveLength(2);
  });

  it("marks a period-scoped family row committed for the matching concrete segment", () => {
    const r = row({
      key: "individer-15plus::Kon",
      variant: "individer-15plus",
      variantLabel: "Individer, 15 år och äldre",
      variantFamily: "individer-15plus",
      variantFamilyLabel: "Individer",
      column: "Kon",
      representation: "Kon",
      renamedColumns: [],
      windows: [
        { from: "1990-01-01", to: "2009-12-31" },
        { from: "2010-01-01", to: "2023-12-31" },
      ],
      variantSegments: [
        {
          variant: "individer-16plus",
          variantLabel: "Individer, 16 år och äldre",
          windows: [{ from: "1990-01-01", to: "2009-12-31" }],
        },
        {
          variant: "individer-15plus",
          variantLabel: "Individer, 15 år och äldre",
          windows: [{ from: "2010-01-01", to: "2023-12-31" }],
        },
      ],
    });
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA 1990-2009",
          register_variant: "scb/lisa/individer-16plus",
          period: { from: 1990, to: 2009 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "integer",
              representation: "Kon",
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b], {
      period: "1990..2009",
      window: [1990, 2009],
    }).get(pickerRowKey(b, r));

    expect(committed).toEqual(
      expect.objectContaining({
        registerVariant: "scb/lisa/individer-16plus",
        representation: "Kon",
      }),
    );
    expect(
      stagedRemoveForCommitted(committed as NonNullable<typeof committed>),
    ).toEqual([
      {
        registerVariant: "scb/lisa/individer-16plus",
        variable: "scb/lisa/dinf",
        representation: "Kon",
      },
    ]);
  });

  it("requires every family segment when no picker period scope is active", () => {
    const r = row({
      key: "individer-15plus::Kon",
      variant: "individer-15plus",
      variantLabel: "Individer, 15 år och äldre",
      variantFamily: "individer-15plus",
      variantFamilyLabel: "Individer",
      column: "Kon",
      representation: "Kon",
      renamedColumns: [],
      windows: [
        { from: "1990-01-01", to: "2009-12-31" },
        { from: "2010-01-01", to: "2023-12-31" },
      ],
      variantSegments: [
        {
          variant: "individer-16plus",
          variantLabel: "Individer, 16 år och äldre",
          windows: [{ from: "1990-01-01", to: "2009-12-31" }],
        },
        {
          variant: "individer-15plus",
          variantLabel: "Individer, 15 år och äldre",
          windows: [{ from: "2010-01-01", to: "2023-12-31" }],
        },
      ],
    });
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA 1990-2009",
          register_variant: "scb/lisa/individer-16plus",
          period: { from: 1990, to: 2009 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "integer",
              representation: "Kon",
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.has(pickerRowKey(b, r))).toBe(false);
  });

  it("scopes a null stored representation to rows overlapping the source period", () => {
    const rows = [
      row({
        key: "ind::OLD",
        column: "OLD",
        representation: "OLD",
        from: "1981-01-01",
        to: "1985-12-31",
        windows: [{ from: "1981-01-01", to: "1985-12-31" }],
        period: "1981 - 1985",
        wirePeriod: "1981..1985",
        renamedColumns: [],
      }),
      row({
        key: "ind::NEW",
        column: "NEW",
        representation: "NEW",
        from: "1986-01-01",
        to: "1995-12-31",
        windows: [{ from: "1986-01-01", to: "1995-12-31" }],
        period: "1986 - 1995",
        wirePeriod: "1986..1995",
        renamedColumns: [],
      }),
    ];
    const b = band(rows);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA",
          register_variant: "scb/lisa/ind",
          period: { from: 1981, to: 1985 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "numeric",
              representation: null,
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.get(pickerRowKey(b, rows[0]))).toEqual(
      expect.objectContaining({ representation: null }),
    );
    expect(committed.has(pickerRowKey(b, rows[1]))).toBe(false);
  });

  it("counts every null-representation row the source period spans", () => {
    const rows = [
      row({
        key: "ind::OLD",
        column: "OLD",
        representation: "OLD",
        from: "1981-01-01",
        to: "1985-12-31",
        windows: [{ from: "1981-01-01", to: "1985-12-31" }],
        period: "1981 - 1985",
        wirePeriod: "1981..1985",
        renamedColumns: [],
      }),
      row({
        key: "ind::NEW",
        column: "NEW",
        representation: "NEW",
        from: "1986-01-01",
        to: "1995-12-31",
        windows: [{ from: "1986-01-01", to: "1995-12-31" }],
        period: "1986 - 1995",
        wirePeriod: "1986..1995",
        renamedColumns: [],
      }),
    ];
    const b = band(rows);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA",
          register_variant: "scb/lisa/ind",
          period: { from: 1981, to: 1995 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "numeric",
              representation: null,
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.get(pickerRowKey(b, rows[0]))).toEqual(
      expect.objectContaining({ representation: null }),
    );
    expect(committed.get(pickerRowKey(b, rows[1]))).toEqual(
      expect.objectContaining({ representation: null }),
    );
  });

  it("gives an imported `_default` source no null-representation coverage", () => {
    // A file written before the sentinel was retired can still carry it, but it
    // is no longer a project period: it denotes no bounds, so it can overlap no
    // row. The picker must not read it as covering everything (an OMITTED
    // `representation` takes the same null path as the explicit null here).
    const rows = [
      row({
        key: "ind::OLD",
        column: "OLD",
        representation: "OLD",
        from: "1981-01-01",
        to: "1985-12-31",
        windows: [{ from: "1981-01-01", to: "1985-12-31" }],
        period: "1981 - 1985",
        wirePeriod: "1981..1985",
        renamedColumns: [],
      }),
      row({
        key: "ind::NEW",
        column: "NEW",
        representation: "NEW",
        from: "1986-01-01",
        to: "1995-12-31",
        windows: [{ from: "1986-01-01", to: "1995-12-31" }],
        period: "1986 - 1995",
        wirePeriod: "1986..1995",
        renamedColumns: [],
      }),
    ];
    const b = band(rows);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA",
          register_variant: "scb/lisa/ind",
          period: "_default",
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "numeric",
              representation: null,
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.size).toBe(0);
  });

  it("skips malformed draft source slots instead of crashing", () => {
    const r = row();
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        null as never,
        { register_variant: 17 } as never,
        {
          name: "LISA",
          register_variant: "scb/lisa/ind",
          period: { from: 1981, to: 1985 },
          bindings: [
            null as never,
            {
              variable: "scb/lisa/dinf",
              type: "numeric",
              representation: "DINF83",
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.get(pickerRowKey(b, r))).toEqual(
      expect.objectContaining({
        representation: "DINF83",
        variable: "scb/lisa/dinf",
      }),
    );
  });
});

describe("periodChangesWithStagedAdds", () => {
  it("preserves same-variant staged add windows when a period change replaces the source period", () => {
    expect(
      periodChangesWithStagedAdds(
        [
          {
            sourceName: "LISA",
            registerVariant: "scb/lisa/ind",
            period: { from: 2012, to: 2014 },
          },
        ],
        [
          {
            registerVariant: "scb/lisa/ind",
            period: { from: 2018, to: 2020 },
          },
          {
            registerVariant: "scb/lisa/arb",
            period: 2020,
          },
        ],
      ),
    ).toEqual([
      {
        sourceName: "LISA",
        registerVariant: "scb/lisa/ind",
        period: [
          { from: 2012, to: 2014 },
          { from: 2018, to: 2020 },
        ],
      },
    ]);
  });

  it("keeps a wider period change instead of narrowing it to the staged add", () => {
    expect(
      periodChangesWithStagedAdds(
        [
          {
            sourceName: "LISA",
            registerVariant: "scb/lisa/ind",
            period: { from: 2000, to: 2020 },
          },
        ],
        [
          {
            registerVariant: "scb/lisa/ind",
            period: { from: 2018, to: 2020 },
          },
        ],
      ),
    ).toEqual([
      {
        sourceName: "LISA",
        registerVariant: "scb/lisa/ind",
        period: { from: 2000, to: 2020 },
      },
    ]);
  });

  it("preserves token add windows when a token period change replaces the source period", () => {
    expect(
      periodChangesWithStagedAdds(
        [
          {
            sourceName: "LISA",
            registerVariant: "scb/lisa/ind",
            period: "2020-Q1",
          },
        ],
        [
          {
            registerVariant: "scb/lisa/ind",
            period: "2020-Q2",
          },
        ],
      ),
    ).toEqual([
      {
        sourceName: "LISA",
        registerVariant: "scb/lisa/ind",
        period: ["2020-Q1", "2020-Q2"],
      },
    ]);
  });

  it("preserves multiple same-variant token add windows with a token period change", () => {
    expect(
      periodChangesWithStagedAdds(
        [
          {
            sourceName: "LISA",
            registerVariant: "scb/lisa/ind",
            period: "2020-Q1",
          },
        ],
        [
          {
            registerVariant: "scb/lisa/ind",
            period: "2020-Q2",
          },
          {
            registerVariant: "scb/lisa/ind",
            period: "2020-Q3",
          },
        ],
      ),
    ).toEqual([
      {
        sourceName: "LISA",
        registerVariant: "scb/lisa/ind",
        period: ["2020-Q1", "2020-Q2", "2020-Q3"],
      },
    ]);
  });
});

describe("finalAddPeriodWires", () => {
  it("resolves each add at the final source period it commits under", () => {
    expect(
      finalAddPeriodWires(
        [{ registerVariant: "scb/lisa/ind", period: 2000 }],
        [],
        [
          { registerVariant: "scb/lisa/ind", period: { from: 2010, to: 2015 } },
          { registerVariant: "scb/rams/std", period: 2019 },
        ],
      ),
    ).toEqual(["2000,2010..2015", "2019"]);
  });

  it("refuses the batch when an add has no period of its own", () => {
    // The open-ended row picked with no `?period` and no project window: its
    // `rowAddPeriod` is unset, and there is no source period to inherit — so the
    // pick would author `period: ""` and an underivable binding type.
    expect(
      finalAddPeriodWires(
        [],
        [],
        [
          { registerVariant: "scb/lisa/ind", period: 2019 },
          { registerVariant: "scb/rams/std", period: "" },
        ],
      ),
    ).toBeNull();
  });

  it("refuses the batch when an add resolves to a non-finite period", () => {
    // `_default` is browse state, never a `Source.period` — the same rule
    // `isStructurallyValidPeriodWire` enforces everywhere a `?period` is committed.
    expect(
      finalAddPeriodWires(
        [],
        [],
        [{ registerVariant: "scb/lisa/ind", period: "_default" }],
      ),
    ).toBeNull();
  });

  it("takes a period-less add's period from the source it extends", () => {
    // The source already carries one, so the union is finite and the pick resolves
    // there — a valid add the refusal must not catch.
    expect(
      finalAddPeriodWires(
        [{ registerVariant: "scb/lisa/ind", period: 2018 }],
        [],
        [{ registerVariant: "scb/lisa/ind", period: "" }],
      ),
    ).toEqual(["2018"]);
  });
});

describe("finalSourcePeriodsForStagedAdds", () => {
  it("resolves add bindings against the source period after merge/replacement", () => {
    const periods = finalSourcePeriodsForStagedAdds(
      [
        {
          registerVariant: "scb/lisa/ind",
          period: 2000,
        },
      ],
      [],
      [
        {
          registerVariant: "scb/lisa/ind",
          period: { from: 2010, to: 2015 },
        },
      ],
    );

    expect(periods.get("scb/lisa/ind")).toEqual([
      2000,
      { from: 2010, to: 2015 },
    ]);
  });

  it("lets a same-batch period replacement define the final source period", () => {
    const periods = finalSourcePeriodsForStagedAdds(
      [
        {
          registerVariant: "scb/lisa/ind",
          period: 2000,
        },
      ],
      [
        {
          sourceName: "LISA",
          registerVariant: "scb/lisa/ind",
          period: { from: 2010, to: 2015 },
        },
      ],
      [
        {
          registerVariant: "scb/lisa/ind",
          period: { from: 2010, to: 2015 },
        },
      ],
    );

    // The change wins outright: without it the add would have coalesced onto the
    // existing 2000 into a two-segment list.
    expect(periods.get("scb/lisa/ind")).toEqual({ from: 2010, to: 2015 });
  });

  it("keeps duplicate register variants aligned with the source that apply will update", () => {
    const periods = finalSourcePeriodsForStagedAdds(
      [
        {
          registerVariant: "scb/lisa/ind",
          period: 2000,
        },
        {
          registerVariant: "scb/lisa/ind",
          period: 2020,
        },
      ],
      [],
      [
        {
          registerVariant: "scb/lisa/ind",
          period: 2010,
        },
      ],
    );

    expect(periods.get("scb/lisa/ind")).toEqual([2000, 2010]);
  });

  it("keeps multiple same-variant token adds in the final source period", () => {
    const periods = finalSourcePeriodsForStagedAdds(
      [
        {
          registerVariant: "scb/lisa/ind",
          period: "2020-Q1",
        },
      ],
      [],
      [
        {
          registerVariant: "scb/lisa/ind",
          period: "2020-Q2",
        },
        {
          registerVariant: "scb/lisa/ind",
          period: "2020-Q3",
        },
      ],
    );

    expect(periods.get("scb/lisa/ind")).toEqual([
      "2020-Q1",
      "2020-Q2",
      "2020-Q3",
    ]);
  });
});

describe("nullBindingCommittedRowKeys", () => {
  it("returns every committed row backed by the same null binding", () => {
    const rows = [
      row({ key: "ind::OLD", column: "OLD", representation: "OLD" }),
      row({ key: "ind::NEW", column: "NEW", representation: "NEW" }),
      row({ key: "arb::OTHER", variant: "arb", column: "OTHER" }),
    ];
    const b = band(rows);
    const committed = [
      {
        key: pickerRowKey(b, rows[0]),
        registerVariant: "scb/lisa/ind",
        variable: "scb/lisa/dinf",
        representation: null,
        sourceName: "LISA",
        sourcePeriod: 2020,
      },
      {
        key: pickerRowKey(b, rows[1]),
        registerVariant: "scb/lisa/ind",
        variable: "scb/lisa/dinf",
        representation: null,
        sourceName: "LISA",
        sourcePeriod: 2020,
      },
      {
        key: pickerRowKey(b, rows[2]),
        registerVariant: "scb/lisa/arb",
        variable: "scb/lisa/dinf",
        representation: null,
        sourceName: "LISA",
        sourcePeriod: 2020,
      },
    ];

    expect(nullBindingCommittedRowKeys(committed, committed[0])).toEqual([
      pickerRowKey(b, rows[0]),
      pickerRowKey(b, rows[1]),
    ]);
  });
});

describe("rowAddSegments (#376 per-concrete-segment fan-out)", () => {
  it("stages an unfolded row as its single variant over its own span", () => {
    const b = band([row()]);
    expect(rowAddSegments(b, b.rows[0], {})).toEqual([
      {
        variant: "ind",
        registerVariant: "scb/lisa/ind",
        periodWire: "1981..1995",
      },
    ]);
  });

  it("fans a folded family with no active scope into every concrete era segment", () => {
    const r = foldedFamilyRow();
    const b = band([r]);
    // No scope → each concrete segment stages as its OWN register_variant over its OWN
    // era window; the head `individer-15plus` never absorbs the predecessor era (#376).
    expect(rowAddSegments(b, r, {})).toEqual([
      {
        variant: "individer-16plus",
        registerVariant: "scb/lisa/individer-16plus",
        periodWire: "1990..2009",
      },
      {
        variant: "individer-15plus",
        registerVariant: "scb/lisa/individer-15plus",
        periodWire: "2010..2023",
      },
    ]);
  });

  it("narrows a period-scoped family add to the concrete era it overlaps, clipped", () => {
    const r = foldedFamilyRow();
    const b = band([r]);
    // A 1995–2000 scope touches only the 16plus era → ONE staged add, for that concrete
    // segment, clipped to the scope; the 15plus-era source is never created (partial
    // family add) and the coordinate is the predecessor variant, not the head (#376).
    expect(rowAddSegments(b, r, { period: "1995..2000" })).toEqual([
      {
        variant: "individer-16plus",
        registerVariant: "scb/lisa/individer-16plus",
        periodWire: "1995..2000",
      },
    ]);
  });
});

// ── The shared staged add → resolve → commit stack (Y-83) ────────────────────
// Hoisted out of the binding leaf + concept group so the register list could use
// it too; these cover the seam the three hosts now share.

/** A minimal `VariableStateModel` — the fields the row enumeration + the type
 * derivation read. */
function leafState(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "individer",
    variant_label: null,
    register_variant_id: 1,
    valid_from: "1990-01-01",
    valid_to: "2023-12-31",
    data_type: "int",
    data_length: null,
    delivery_column_name: "Kon",
    source_register_text: null,
    value_set_version_label: "",
    value_set_id: 7,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
    ...over,
  };
}

/** The same two deliveries as `konStates`, in the aggregate form the REGISTER
 * list receives them (`BindingChild.deliveries`: one MIN/MAX coverage per
 * (variant, column)). */
const konDeliveries = [
  {
    variant: "hushall",
    coverage: {
      coverage_from: "1990-01-01",
      coverage_to: "2023-12-31",
      open_ended: false,
    },
  },
  {
    variant: "individer",
    coverage: {
      coverage_from: "1990-01-01",
      coverage_to: "2023-12-31",
      open_ended: false,
    },
  },
];

/** `Kon` as the VARIABLE page sees it: one state per delivering variant. Two
 * PARALLEL variants, so the leaf enumerates two rows — the same two the register
 * list's one tickable column stands for. */
const konStates = [
  leafState({ state_id: 1, variant: "hushall" }),
  leafState({ state_id: 2, variant: "individer" }),
];

const konBandKey = "scb/lisa/kon";

function konBandOf(rows: PickerRepresentation[]): StagedPickerBand {
  return { key: konBandKey, registerPrefix: "scb/lisa", rows };
}

function picksOf(bandRows: PickerRepresentation[]): StagedPick[] {
  const b = konBandOf(bandRows);
  return b.rows.map((row) => ({ band: b, row }));
}

const SEED = { regMetaVersion: "reg_meta/v1.0.0", steward: "global" };

/** Resolve every `?period` GET to the picked variant's own state, so the staged
 * binding derives a concrete type (the #991 write-once shape). */
function stubResolve(states: VariableStateModel[]): void {
  vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
    const variant = typeof params?.variant === "string" ? params.variant : "";
    return {
      states: states.filter((s) => !variant || s.variant === variant),
    } as never;
  });
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("stagedAddCandidates", () => {
  it("fans a folded family row out to one candidate per concrete era", () => {
    const b = band([foldedFamilyRow()]);
    expect(
      stagedAddCandidates({ band: b, row: b.rows[0] }, {}).map((c) => [
        c.registerVariant,
        c.periodWire,
        c.period,
      ]),
    ).toEqual([
      ["scb/lisa/individer-16plus", "1990..2009", { from: 1990, to: 2009 }],
      ["scb/lisa/individer-15plus", "2010..2023", { from: 2010, to: 2023 }],
    ]);
  });

  it("stages a register-list column exactly as the variable page's own rows do", () => {
    // The Y-83 claim, at the staging seam: the register list's ONE tickable
    // `Kon` column and the variable page's TWO variant rows fan out to the same
    // (register_variant, period) adds, so the two surfaces author the same thing.
    const scope = { period: null, window: [2018, 2023] as [number, number] };
    const fields = (picks: StagedPick[]) =>
      picks.flatMap((pick) =>
        stagedAddCandidates(pick, scope).map((c) => [
          c.registerVariant,
          c.periodWire,
        ]),
      );
    expect(fields(picksOf(deliveryColumnRows("Kon", konDeliveries)))).toEqual(
      fields(picksOf(pickerRepresentations(konStates))),
    );
  });
});

describe("applyStagedPicks", () => {
  it("commits a register-list pick to the same project_data.json as the leaf's", async () => {
    stubResolve(konStates);
    const scope = { period: null, window: [2018, 2023] as [number, number] };
    const ctx = { scope, seed: SEED, cancelled: () => false };

    projectStore.newProject({
      reg_meta_version: SEED.regMetaVersion,
      steward: SEED.steward,
    });
    const fromLeaf = await applyStagedPicks(
      {
        adds: picksOf(pickerRepresentations(konStates)),
        removes: [],
        periodChanges: [],
      },
      ctx,
    );
    const leafDraft = JSON.stringify(projectStore.draft);

    projectStore.newProject({
      reg_meta_version: SEED.regMetaVersion,
      steward: SEED.steward,
    });
    const fromRegister = await applyStagedPicks(
      {
        adds: picksOf(deliveryColumnRows("Kon", konDeliveries)),
        removes: [],
        periodChanges: [],
      },
      ctx,
    );

    expect(fromLeaf).toEqual({
      kind: "applied",
      outcome: { added: 2, removed: 0, periodChanged: 0 },
    });
    expect(fromRegister).toEqual(fromLeaf);
    expect(JSON.stringify(projectStore.draft)).toBe(leafDraft);
  });

  it("refuses the whole batch, unmutated, when an add resolves no finite period", async () => {
    stubResolve(konStates);
    // Delivered open-ended and no window to clip it to: there is no finite period
    // to commit or to resolve the binding at, so the batch is refused BEFORE the
    // store is touched — never a half-authored `period: ""` source (Y-58).
    const openEnded = deliveryColumnRows("Kon", [
      {
        variant: "individer",
        coverage: {
          coverage_from: "2018-01-01",
          coverage_to: null,
          open_ended: true,
        },
      },
    ]);
    projectStore.newProject({
      reg_meta_version: SEED.regMetaVersion,
      steward: SEED.steward,
    });
    const before = JSON.stringify(projectStore.draft);

    const result = await applyStagedPicks(
      { adds: picksOf(openEnded), removes: [], periodChanges: [] },
      {
        scope: { period: null, window: null },
        seed: SEED,
        cancelled: () => false,
      },
    );

    expect(result).toEqual({ kind: "period-required" });
    expect(JSON.stringify(projectStore.draft)).toBe(before);
    expect(getCatalogNode).not.toHaveBeenCalled();
  });

  it("abandons a pick whose host is gone rather than authoring into a draft it left", async () => {
    stubResolve(konStates);
    projectStore.newProject({
      reg_meta_version: SEED.regMetaVersion,
      steward: SEED.steward,
    });
    const before = JSON.stringify(projectStore.draft);

    const result = await applyStagedPicks(
      {
        adds: picksOf(deliveryColumnRows("Kon", konDeliveries)),
        removes: [],
        periodChanges: [],
      },
      {
        scope: { period: null, window: [2018, 2023] },
        seed: SEED,
        cancelled: () => true,
      },
    );

    expect(result).toEqual({ kind: "abandoned" });
    expect(JSON.stringify(projectStore.draft)).toBe(before);
  });

  it("has nothing to confirm for an empty batch", async () => {
    expect(
      await applyStagedPicks(
        { adds: [], removes: [], periodChanges: [] },
        { scope: {}, seed: SEED, cancelled: () => false },
      ),
    ).toEqual({ kind: "applied", outcome: null });
  });
});
