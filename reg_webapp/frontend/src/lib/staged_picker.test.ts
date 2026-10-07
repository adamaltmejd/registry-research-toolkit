import { afterEach, describe, expect, it, vi } from "vitest";
import type { PickerRepresentation } from "./catalog";
import type { ProjectData } from "./project_data";
import {
  committedPickerRows,
  finalAddPeriodWires,
  pickerRowKey,
  rowAddSegments,
  type StagedPickerBand,
  stagedRemoveForCommitted,
  windowsOverlapPeriod,
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
});

describe("windowsOverlapPeriod", () => {
  const eras = (...windows: [string, string][]) =>
    windows.map(([from, to]) => ({ from, to }));

  it("reads a committed source's period era by era", () => {
    // A source committed over an INTERRUPTED column carries the #307 comma-union,
    // so a name delivered only in the gap year was never committed — the union's
    // outer span says it was.
    const committed = [
      { from: 1995, to: 1996 },
      { from: 1998, to: 2015 },
    ];
    expect(
      windowsOverlapPeriod(eras(["1997-01-01", "1997-12-31"]), committed),
    ).toBe(false);
    expect(
      windowsOverlapPeriod(eras(["1996-01-01", "1997-12-31"]), committed),
    ).toBe(true);
  });
});

describe("finalAddPeriodWires", () => {
  it("accepts year-independent selection only with a concrete variant and unmixed scope", () => {
    expect(
      finalAddPeriodWires(
        [],
        [{ registerVariant: "scb/lisa/country-groups", period: "_default" }],
      ),
    ).toEqual(["_default"]);
    expect(
      finalAddPeriodWires(
        [],
        [{ registerVariant: "scb/lisa/_default", period: "_default" }],
      ),
    ).toBeNull();
    expect(
      finalAddPeriodWires(
        [{ registerVariant: "scb/lisa/country-groups", period: 2020 }],
        [{ registerVariant: "scb/lisa/country-groups", period: "_default" }],
      ),
    ).toBeNull();
    expect(
      finalAddPeriodWires(
        [{ registerVariant: "scb/lisa/country-groups", period: "_default" }],
        [{ registerVariant: "scb/lisa/country-groups", period: 2020 }],
      ),
    ).toBeNull();
    expect(
      finalAddPeriodWires(
        [],
        [
          { registerVariant: "scb/lisa/country-groups", period: "_default" },
          { registerVariant: "scb/lisa/country-groups", period: 2020 },
        ],
      ),
    ).toBeNull();
  });

  it("takes a period-less add's period from the source it extends", () => {
    // The source already carries one, so the union is finite and the pick resolves
    // there — a valid add the refusal must not catch.
    expect(
      finalAddPeriodWires(
        [{ registerVariant: "scb/lisa/ind", period: 2018 }],
        [{ registerVariant: "scb/lisa/ind", period: "" }],
      ),
    ).toEqual(["2018"]);
  });
});

describe("rowAddSegments (#376 per-concrete-segment fan-out)", () => {
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
        outsideScope: false,
      },
    ]);
  });

  it("marks every segment outside a scope that misses each era, with no period", () => {
    const r = foldedFamilyRow();
    const b = band([r]);
    // No era reaches 1970–1980: nothing to commit, and nothing invented from the
    // eras' own spans — the Apply is refused by name.
    expect(
      rowAddSegments(b, r, { window: [1970, 1980] }).map((s) => [
        s.periodWire,
        s.outsideScope,
      ]),
    ).toEqual([
      [null, true],
      [null, true],
    ]);
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});
